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
        std::cerr << "\nProcessing query " << Q << '\n';

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
            std::cerr << " * relation r" << Q.relations[i] << " has ";
            auto &entry = join_table[i];
            if (entry.attr.attribute == std::size_t(-1)) {
                std::cerr << "multiple join attributes";
                is_simple = false;
            } else {
                std::cerr << entry.count << " joins with attribute a" << entry.attr.attribute;
            }
            std::cerr << '\n';
        }
        if (not is_simple)
            std::cerr << " => in-place aggregation join not possible, must materialize intermediate results\n";
#endif

        if (not is_simple) {
            /* TODO other query processing strategy */
            delete[] join_table;
            continue;
        }

        /* If the query is "simple", and we can do an in-place aggregation join, we only need to build one hash table
         * where we aggregate all joins.  Use the smallest relation for that task.  */
        std::cerr << " => perform in-place aggregation join\n\n";

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
        break;  // FIXME stop after first query

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
    struct agg_t
    {
        agg_t(std::size_t offset, std::size_t attribute) : offset(offset), attribute(attribute) { }
        std::size_t offset; ///< the offset of this aggregate within the values vector of an index entry
        std::size_t attribute; ///< the attribute id of this aggregate
    };
    std::vector<agg_t> aggregates;
    aggregates.reserve(8);
    for (std::size_t i = 0; i != Q.projections.size(); ++i) {
        auto P = Q.projections[i];
        if (P.relation == build_relation_id)
            aggregates.emplace_back(/* offset */ num_relations + i, /* attribute */ P.attribute);
    }

    /* Insert the build relation.  TODO Handle filters. */
    for (std::size_t i = 0, end = build_relation.rows(); i != end; ++i) {
        uint64_t *value = I[build_relation.get_column(build_attribute_id)[i]];
        ++value[build_relation_id]; // update the counter for this relation
        for (auto agg : aggregates)
            value[agg.offset] += build_relation.get_column(agg.attribute)[i];
    }

    /* Evaluate all joins.  TODO Handle filters. */
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

        /* Evaluate the join. */
        for (std::size_t i = 0, end = R.rows(); i != end; ++i) {
            auto it = I.find(R.get_column(probe.attribute)[i]);
            if (it == I.end()) continue;
            uint64_t *values = it->second;
            ++values[probe.relation]; // update the counter for this relation
            for (auto agg : aggregates)
                values[agg.offset] += R.get_column(agg.attribute)[i];
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
        if (i != 0) std::cerr << " ";
        auto P = Q.projections[i];
        std::cerr << "SUM(r" << Q.relations[P.relation] << ".c" << P.attribute << ")=" << sums[i];
    }
    std::cerr << "\ntime: " << duration_cast<nanoseconds>(time_stop - time_start).count() / 1e6 << " milliseconds";
    std::cerr << std::endl;

    delete[] sums;
}
