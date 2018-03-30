#include "QueryExecutor.hpp"

#include "Catalog.hpp"
#include "Index.hpp"


/*-- Helper types & functions ----------------------------------------------------------------------------------------*/
struct agg_t
{
    agg_t(std::size_t offset, std::size_t attribute) : offset(offset), attribute(attribute) { }
    std::size_t offset; ///< the offset of this aggregate within the values vector of an index entry
    std::size_t attribute; ///< the attribute id of this aggregate
};

struct no_operation { bool operator()(uint64_t, uint64_t) const { return true; } };

template<typename Operation>
struct Filter
{
    std::size_t attribute;
    uint64_t num;

    Filter(std::size_t attribute, uint64_t num) : attribute(attribute), num(num) { }

    bool operator()(const Relation &R, std::size_t i) const {
        return Operation{}(R.get_column(attribute)[i], num);
    }
};

template<typename Op>
void fill_index(Index &I, const Relation &R, std::size_t relation_id, std::size_t attribute_id,
                std::vector<agg_t> &aggregates, Filter<Op> F)
{
    for (std::size_t i = 0, end = R.rows(); i != end; ++i) {
        if (not F(R, i)) continue;
        const uint64_t join_key = R.get_column(attribute_id)[i];
        uint64_t *value = I[join_key];
        ++value[relation_id]; // update the counter for this relation
        for (auto agg : aggregates)
            value[agg.offset] += R.get_column(agg.attribute)[i];
    }
}

template<typename Op>
void probe_index(Index &I, const Relation &R, std::size_t relation_id, std::size_t attribute_id,
                 std::vector<agg_t> &aggregates, Filter<Op> F)
{
    for (std::size_t i = 0, end = R.rows(); i != end; ++i) {
        if (not F(R, i)) continue;
        const uint64_t join_key = R.get_column(attribute_id)[i];
        auto it = I.find(join_key);
        if (it == I.end()) continue;
        uint64_t *value = it->second;
        ++value[relation_id]; // update the counter for this relation
        for (auto agg : aggregates)
            value[agg.offset] += R.get_column(agg.attribute)[i];
    }
}

template<typename Op>
float sample_filter(const Relation &R, Filter<Op> F)
{
    constexpr std::size_t SAMPLE_SIZE = 1024;
    std::size_t qualified = 0;
    for (std::size_t i = 0; i != SAMPLE_SIZE; ++i)
        qualified += F(R, i);
    return float(qualified) / SAMPLE_SIZE;
}

/*-- QueryExecutor ---------------------------------------------------------------------------------------------------*/
void QueryExecutor::execute()
{
    const Catalog &C = Catalog::Get();
    const std::size_t num_relations = query.relations.size();

    /* Check whether we can do an in-place aggregation join.  This is only possible, if for each relation, only one
     * attribute is used to join.  This means, that all relations are joined by one "common" attribute.  */
    struct join_count_t
    {
        QueryDescription::Attr attr;
        std::size_t count;
    };
    join_count_t *join_counters = new join_count_t[num_relations]();
    auto update = [=](QueryDescription::Attr A) {
        auto &e = join_counters[A.relation];
        if (e.count == 0) {
            ++e.count;
            e.attr = A;
        } else if (e.count == std::size_t(-1)) {
            /* nothing to do */
        } else if (e.attr.attribute != A.attribute) {
            e.attr.attribute = e.count = std::size_t(-1);
        } else {
            assure(e.count != std::size_t(-1));
            assure(e.attr.relation == A.relation);
            assure(e.attr.attribute == A.attribute);
            ++e.count;
        }
    };
    for (auto J : query.joins) {
        update(J.lhs);
        update(J.rhs);
    }

    bool is_simple = true;
    for (std::size_t i = 0; i != num_relations; ++i)
        is_simple &= join_counters[i].attr.attribute != std::size_t(-1);

    if (not is_simple) {
        delete[] join_counters;
        return;
    }

    /* Choose build relation.  TODO use selectivities of filters. */
    std::size_t build_relation;
    std::size_t relation_size = std::numeric_limits<std::size_t>::max();
    for (std::size_t i = 0; i != query.relations.size(); ++i) {
        auto &rel = C[query.relations[i]];
        if (rel.rows() < relation_size) {
            relation_size = rel.rows();
            build_relation = i;
        }
    }
    std::size_t build_attribute = join_counters[build_relation].attr.attribute;
    delete[] join_counters;

    in_place_aggregation_join(build_relation, build_attribute);
}

void QueryExecutor::in_place_aggregation_join(std::size_t build_relation, std::size_t build_attribute)
{
    const Catalog &C = Catalog::Get();
    const std::size_t num_relations = query.relations.size();
    std::vector<agg_t> aggregates;
    aggregates.reserve(8);

    /* Create the index.  TODO Use a better size estimate. */
    Index I(uint64_t(-1), C[query.relations[build_relation]].rows());
    I.add_fields(query.relations.size()); // add a counter for each relation
    I.add_fields(query.projections.size()); // add a field for each aggregate

    /* Collect the attributes to aggregate when building the relation. */
    for (std::size_t i = 0; i != query.projections.size(); ++i) {
        auto P = query.projections[i];
        if (P.relation == build_relation)
            aggregates.emplace_back(/* offset */ num_relations + i, /* attribute */ P.attribute);
    }

    /* Fill the index with the build relation.  Use a filter, if available. */
    {
        const Relation &BR = C[query.relations[build_relation]]; // get a handle on the build relation
        bool has_filter = false;
        QueryDescription::Filter the_filter;
        for (auto F : query.filters) {
            if (F.lhs.relation == build_relation) {
                the_filter = F;
                has_filter = true;
                break;
            }
        }
        if (has_filter) {
            switch (the_filter.op) {
                case '=':
                    fill_index(I, BR, build_relation, build_attribute, aggregates,
                               Filter<std::equal_to<uint64_t>>(the_filter.lhs.attribute, the_filter.value));
                    break;
                case '<':
                    fill_index(I, BR, build_relation, build_attribute, aggregates,
                               Filter<std::less<uint64_t>>(the_filter.lhs.attribute, the_filter.value));
                    break;
                case '>':
                    fill_index(I, BR, build_relation, build_attribute, aggregates,
                               Filter<std::greater<uint64_t>>(the_filter.lhs.attribute, the_filter.value));
                    break;
            }
        } else {
            fill_index(I, BR, build_relation, build_attribute, aggregates, Filter<no_operation>(0, 0));
        }
    }

    /* Evaluate all joins.  Use a filter, if available. */
    for (auto J : query.joins) {
        auto probe = J.lhs.relation == build_relation ? J.rhs : J.lhs;
        auto &R = C[query.relations[probe.relation]];

        /* Collect the aggregates to perform during this join. */
        aggregates.clear();
        for (std::size_t i = 0; i != query.projections.size(); ++i) {
            auto P = query.projections[i];
            if (P.relation == probe.relation)
                aggregates.emplace_back(/* offset */ num_relations + i, /* attribute */ P.attribute);
        }

        {
            bool has_filter = false;
            QueryDescription::Filter the_filter;
            for (auto F : query.filters) {
                if (F.lhs.relation == probe.relation) {
                    the_filter = F;
                    has_filter = true;
                    break;
                }
            }

            if (has_filter) {
                switch (the_filter.op) {
                    case '=':
                        probe_index(I, R, probe.relation, probe.attribute, aggregates,
                                    Filter<std::equal_to<uint64_t>>(the_filter.lhs.attribute, the_filter.value));
                        break;
                    case '<':
                        probe_index(I, R, probe.relation, probe.attribute, aggregates,
                                    Filter<std::less<uint64_t>>(the_filter.lhs.attribute, the_filter.value));
                        break;
                    case '>':
                        probe_index(I, R, probe.relation, probe.attribute, aggregates,
                                    Filter<std::greater<uint64_t>>(the_filter.lhs.attribute, the_filter.value));
                        break;
                }
            } else {
                probe_index(I, R, probe.relation, probe.attribute, aggregates, Filter<no_operation>(0, 0));
            }
        }
    }

    /* After all in-place aggregation joins, compute the final check sums. */
    for (auto &entry : I) {
        uint64_t *values = entry.second;

        /* Compute how many join results (if materialized) are aggregated in this entry. */
        uint64_t value_count = 1;
        for (std::size_t i = 0; i != num_relations; ++i)
            value_count *= values[i];
        result_[0] += value_count;

        for (std::size_t i = 0; i != query.projections.size(); ++i) {
            auto P = query.projections[i];
            /* Compute the count of this aggregate. */
            uint64_t count = 1;
            for (std::size_t j = 0; j != num_relations; ++j)
                count *= P.relation == j ? 1 : values[j];
            result_[i + 1] += count * values[num_relations + i];
        }
    }
}

void QueryExecutor::compute_size_estimates()
{
    std::cerr << ">>> QueryExecutor::compute_size_estimates()\n";
    const Catalog &C = Catalog::Get();
    for (std::size_t i = 0; i != query.relations.size(); ++i) {
        const auto &R = C[query.relations[i]];
        size_estimates_[i] = R.rows();
        std::cerr << "  - relation r" << query.relations[i] << " has " << R.rows() << " rows\n";
    }
    for (auto F : query.filters) {
        const std::size_t relation = F.lhs.relation;
        auto &R = C[query.relations[relation]];
        float selectivity;
        switch (F.op) {
            default: unreachable("invalid filter");
            case '=':
                selectivity = sample_filter(R, Filter<std::equal_to<uint64_t>>(F.lhs.attribute, F.value));
                break;
            case '<':
                selectivity = sample_filter(R, Filter<std::less<uint64_t>>(F.lhs.attribute, F.value));
                break;
            case '>':
                selectivity = sample_filter(R, Filter<std::greater<uint64_t>>(F.lhs.attribute, F.value));
                break;
        }
        std::size_t expected_size = R.rows() * selectivity;
#ifndef NDEBUG
        std::cerr << "  - filter relation r" << query.relations[F.lhs.relation] << " with " << F
                  << ", sampled selectivity is " << selectivity * 100 << "%, expected size is " << expected_size << '\n';
#endif
        size_estimates_[relation] = std::min(size_estimates_[relation], expected_size);
    }
}
