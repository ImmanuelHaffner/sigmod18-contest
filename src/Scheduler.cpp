#include "Scheduler.hpp"

#include "HashTable.hpp"
#include "Index.hpp"
#include "QueryPlanning.hpp"
#include <algorithm>
#include <chrono>
#include <iomanip>
#include <iostream>
#include <limits>
#include <utility>


using namespace std::chrono;


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

    bool operator()(Relation &R, std::size_t i) const {
        return Operation{}(R.get_column(attribute)[i], num);
    }
};

template<typename Op>
void fill_index(Index &I, Relation &R, std::size_t relation_id, std::size_t attribute_id,
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
void probe_index(Index &I, Relation &R, std::size_t relation_id, std::size_t attribute_id,
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

void Scheduler::execute(const std::vector<QueryDescription> &batch)
{
    /* TODO
     * Tasks:
     *   1. Identify frequently used relations.
     *   2. Decide for which relation/attribute to build an index.
     *   3. For each query, construct a query plan.
     *   4. Distribute the query plans to different threads and execute them.
     *   5. Join the threads and emit the results in order.
     */

    /* Compute a query plan. */
    for (const auto &Q : batch) {
#ifndef NDEBUG
        std::cerr << "\nProcessing query " << Q << '\n';
#endif

        /* Check whether we can do an in-place aggregation join.  This is only possible, if for each relation, only one
         * attribute is used to join.  This means, that all relations are joined by one "common" attribute.  */
        const std::size_t num_relations = Q.relations.size();
        struct join_order_t
        {
            QueryDescription::Attr attr;
            std::size_t count;
        };
        join_order_t *join_table = new join_order_t[num_relations]();
        auto update = [=](QueryDescription::Attr A) {
            auto &e = join_table[A.relation];
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
        for (auto J : Q.joins) {
            update(J.lhs);
            update(J.rhs);
        }

        bool is_simple = true;
        for (std::size_t i = 0; i != num_relations; ++i)
            is_simple &= join_table[i].attr.attribute != std::size_t(-1);

#ifndef NDEBUG
        for (std::size_t i = 0; i != num_relations; ++i) {
            auto &entry = join_table[i];
            if (entry.attr.attribute == std::size_t(-1))
                is_simple = false;
        }
        if (not is_simple)
            std::cerr << " => in-place aggregation join not possible, must materialize intermediate results\n";
        else
            std::cerr << " => perform in-place aggregation join\n";
#endif

        if (not is_simple) {
            /* TODO other query processing strategy */
            std::cout << "<not supported>\n";
            delete[] join_table;
            continue;
        }

        /* Choose build relation.  TODO use selectivities of filters. */
        std::size_t build_relation_id;
        std::size_t relation_size = std::numeric_limits<std::size_t>::max();
        for (std::size_t i = 0; i != Q.relations.size(); ++i) {
            auto &rel = catalog[Q.relations[i]];
            if (rel.rows() < relation_size) {
                relation_size = rel.rows();
                build_relation_id = i;
            }
        }
        std::size_t build_attribute_id = join_table[build_relation_id].attr.attribute;
        delete[] join_table;

        in_place_aggregation_join(Q, build_relation_id, build_attribute_id);
    }
}

void Scheduler::in_place_aggregation_join(const QueryDescription &Q,
                                          std::size_t build_relation_id,
                                          std::size_t build_attribute_id)
{
    const auto time_start = high_resolution_clock::now();
    const auto num_relations = Q.relations.size();

    /* Get a handle on the relation. */
    Relation &build_relation = catalog[Q.relations[build_relation_id]];

    /* Create the index.  TODO Use a better size estimate. */
    Index I(uint64_t(-1), build_relation.rows());

    /* Add a counter for each relation of the query. */
    I.add_fields(Q.relations.size());
    /* Add a counter for each aggregate. */
    I.add_fields(Q.projections.size());

    /* Collect the attributes to aggregate. */
    std::vector<agg_t> aggregates;
    aggregates.reserve(8);
    for (std::size_t i = 0; i != Q.projections.size(); ++i) {
        auto P = Q.projections[i];
        if (P.relation == build_relation_id)
            aggregates.emplace_back(/* offset */ num_relations + i, /* attribute */ P.attribute);
    }

    /* Fill the index with the build relation.  Use a filter, if available. */
    {
        bool has_filter = false;
        QueryDescription::Filter the_filter;
        for (auto F : Q.filters) {
            if (F.lhs.relation == build_relation_id) {
                the_filter = F;
                has_filter = true;
                break;
            }
        }

        if (has_filter) {
            switch (the_filter.op) {
                case '=':
                    fill_index(I, build_relation, build_relation_id, build_attribute_id, aggregates,
                               Filter<std::equal_to<uint64_t>>(the_filter.lhs.attribute, the_filter.value));
                    break;
                case '<':
                    fill_index(I, build_relation, build_relation_id, build_attribute_id, aggregates,
                               Filter<std::less<uint64_t>>(the_filter.lhs.attribute, the_filter.value));
                    break;
                case '>':
                    fill_index(I, build_relation, build_relation_id, build_attribute_id, aggregates,
                               Filter<std::greater<uint64_t>>(the_filter.lhs.attribute, the_filter.value));
                    break;
            }
        } else {
            fill_index(I, build_relation, build_relation_id, build_attribute_id, aggregates, Filter<no_operation>(0, 0));
        }
    }

    /* Evaluate all joins.  Use a filter, if available. */
    for (auto J : Q.joins) {
        auto probe = J.lhs.relation == build_relation_id ? J.rhs : J.lhs;
        auto &R = catalog[Q.relations[probe.relation]];

        /* Collect the aggregates to perform during this join. */
        aggregates.clear();
        for (std::size_t i = 0; i != Q.projections.size(); ++i) {
            auto P = Q.projections[i];
            if (P.relation == probe.relation)
                aggregates.emplace_back(/* offset */ num_relations + i, /* attribute */ P.attribute);
        }

        {
            bool has_filter = false;
            QueryDescription::Filter the_filter;
            for (auto F : Q.filters) {
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
    uint64_t *sums = new uint64_t[Q.projections.size()]();
    std::size_t total_count = 0; // total number of join results (if materialized)

    for (auto &entry : I) {
        uint64_t *values = entry.second;

        /* Compute how many join results (if materialized) are aggregated in this entry. */
        uint64_t value_count = 1;
        for (std::size_t i = 0; i != num_relations; ++i)
            value_count *= values[i];
        total_count += value_count;

        for (std::size_t i = 0; i != Q.projections.size(); ++i) {
            auto P = Q.projections[i];
            /* Compute the count of this aggregate. */
            uint64_t count = 1;
            for (std::size_t j = 0; j != num_relations; ++j)
                count *= P.relation == j ? 1 : values[j];
            sums[i] += count * values[num_relations + i];
        }
    }
    const auto time_stop = high_resolution_clock::now();

    for (std::size_t i = 0; i != Q.projections.size(); ++i) {
        if (i != 0) std::cout << " ";
        if (total_count)
            std::cout << sums[i];
        else
            std::cout << "NULL";
    }
    std::cout << std::endl;
#ifndef NDEBUG
    std::cerr << "time: " << duration_cast<nanoseconds>(time_stop - time_start).count() / 1e6 << " milliseconds";
    std::cerr << std::endl;
#endif

    delete[] sums;
}
