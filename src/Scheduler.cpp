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


void Scheduler::execute(std::vector<QueryDescription> &batch)
{
    /* TODO
     * Tasks:
     *   1. Identify frequently used relations.
     *   2. Decide for which relation/attribute to build an index.
     *   3. For each query, construct a query plan.
     *   4. Distribute the query plans to different threads and execute them.
     *   5. Join the threads and emit the results in order.
     */

    auto time_begin = high_resolution_clock::now();

    /* Compute a query plan. */
    for (auto &Q : batch) {
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
        Relation &build_relation = catalog[Q.relations[build_relation_id]];
        std::cerr << "choose build relation r" << Q.relations[build_relation_id] << " (" << build_relation.rows() << " rows)\n";

        /* Get the build attribute. */
        std::size_t build_attribute_id = join_table[build_relation_id].attr.attribute;
        std::cerr << "  ` build HT on attribute a" << build_attribute_id << '\n';

        /* Create the index. */
        Index I(uint64_t(-1), build_relation.rows());

        /* Add a counter for each relation. */
        for (auto r : Q.relations) {
            (void) r;
            I.add_field();
        }

        /* Add a counter for each aggregate. */
        for (auto p : Q.projections) {
            (void) p;
            I.add_field();
        }
        std::cerr << "    ` created index with " << I.num_fields() << " fields: " << Q.relations.size()
                  << " for counters and " << Q.projections.size() << " for aggregates\n";

        /* Collect the attributes to aggregate. */
        std::vector<std::pair</* vector offset */ std::size_t, /* attribute id */ std::size_t>> aggregates;
        aggregates.reserve(8);
        std::cerr << "    ` insert records from r" << Q.relations[build_relation_id] << '\n';
        for (std::size_t i = 0; i != Q.projections.size(); ++i) {
            auto P = Q.projections[i];
            if (P.relation == build_relation_id) {
                std::cerr << "      ` aggregate on r" << Q.relations[P.relation] << ".a" << P.attribute << " in slot " << num_relations + i << '\n';
                aggregates.emplace_back(/* offset */ num_relations + i, /* attribute id */ P.attribute);
            }
        }

        /* Insert build relation. */
        for (std::size_t i = 0; i != build_relation.rows(); ++i) {
            uint64_t *value = I[build_relation.get_column(build_attribute_id)[i]];
            ++value[build_relation_id]; // update the counter for this relation
            /* update aggregates */
            for (auto agg : aggregates)
                value[agg.first] += build_relation.get_column(agg.second)[i];
        }

#if 1
        {
            std::cerr << '\n';
            auto it = I.cbegin();
            for (std::size_t i = 0; i != 13; ++i) {
                auto &entry = *it++;
                std::cerr << std::setw(5) << entry.first << "  |--->  ";
                for (std::size_t i = 0; i != Q.relations.size(); ++i) {
                    if (i != 0) std::cerr << ", ";
                    std::cerr << "count_r" << Q.relations[i] << " = " << entry.second[i];
                }
                for (auto agg : aggregates)
                    std::cerr << ", SUM(r" << Q.relations[build_relation_id] << ".a" << agg.second << ") = " << std::setw(8) << entry.second[agg.first];
                std::cerr << '\n';
            }
            std::cerr << '\n';
        }
#endif

        /* Evaluate all joins. */
        for (auto J : Q.joins) {
            auto probe = J.lhs.relation == build_relation_id ? J.rhs : J.lhs;
            auto &rel = catalog[Q.relations[probe.relation]];
            std::cerr << "probing relation r" << Q.relations[probe.relation] << " (" << rel.rows() << " rows) against the hash table\n";

            std::size_t current_agg_count = aggregates.size();
            std::cerr << "  ` currently has " << current_agg_count << " aggregations\n";
            for (std::size_t i = 0; i != Q.projections.size(); ++i) {
                auto P = Q.projections[i];
                if (P.relation == probe.relation) {
                    std::cerr << "  ` aggregate on r" << Q.relations[P.relation] << ".a" << P.attribute << " in slot " << num_relations + i << '\n';
                    aggregates.emplace_back(/* offset */ num_relations + i, /* attribute id */ P.attribute);
                }
            }
            std::cerr << "  ` added " << (aggregates.size() - current_agg_count) << " aggregations, now has " << aggregates.size() << " aggregations\n";

            for (std::size_t i = 0; i != rel.rows(); ++i) {
                auto it = I.find(rel.get_column(probe.attribute)[i]);
                if (it == I.end()) continue;
                uint64_t *values = it->second;
                ++values[probe.relation]; // update the counter for this relation
                for (std::size_t i = current_agg_count; i != aggregates.size(); ++i) {
                    auto &agg = aggregates[i];
                    values[agg.first] += rel.get_column(agg.second)[i];
                }
            }
        }

#if 1
        {
            std::cerr << '\n';
            auto it = I.cbegin();
            for (std::size_t i = 0; i != 13; ++i) {
                auto &entry = *it++;
                std::cerr << std::setw(5) << entry.first << "  |--->  ";
                for (std::size_t i = 0; i != Q.relations.size(); ++i) {
                    if (i != 0) std::cerr << ", ";
                    std::cerr << "count_r" << Q.relations[i] << " = " << entry.second[i];
                }
                for (auto agg : aggregates)
                    std::cerr << ", SUM(r" << Q.relations[build_relation_id] << ".a" << agg.second << ") = " << std::setw(8) << entry.second[agg.first];
                std::cerr << '\n';
            }
            std::cerr << '\n';
        }
#endif

        /* After all in-place aggregation joins, compute the final check sums. */
        std::vector<uint64_t> sums(Q.projections.size());

        //auto it = I.cbegin();
        //for (std::size_t i = 0; i != 13; ++i) {
            //auto &entry = *it++;
        std::size_t total_count = 0; // total number of join results (if materialized)
        for (auto &entry : I) {
            uint64_t *values = entry.second;
            for (std::size_t i = 0; i != Q.projections.size(); ++i) {
                auto P = Q.projections[i];
                /* multiply the counts */
                uint64_t count = 1;
                for (std::size_t j = 0; j != num_relations; ++j) {
                    if (P.relation == j) continue;
                    count *= values[j];
                }
                total_count += count;
                /* multiply the count with the aggregate */
                uint64_t sum = count * values[num_relations + i];
#if 0
                std::cerr << "  - evaluate entry " << entry.first << " for SUM(r" << Q.relations[P.relation]
                          << ".a" << P.attribute << ") = " << count << " * " << values[num_relations + i]
                          << " = " << sum << '\n';
#endif
                sums[i] += sum;
            }
        }

        std::cerr << "total count = " << total_count << '\n';
        for (std::size_t i = 0; i != Q.projections.size(); ++i) {
            auto P = Q.projections[i];
            std::cerr << "SUM(r" << Q.relations[P.relation] << ".a" << P.attribute << ") = " << sums[i] << '\n';
        }


        delete[] join_table;

        break;  // FIXME stop after first query
    }

    auto time_end = high_resolution_clock::now();
    std::cerr << "time: " << duration_cast<nanoseconds>(time_end - time_begin).count() / 1e6 << " milliseconds\n";
}
