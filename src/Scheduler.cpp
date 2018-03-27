#include "Scheduler.hpp"

#include "HashTable.hpp"
#include "QueryPlanning.hpp"
#include <algorithm>
#include <iomanip>
#include <iostream>
#include <limits>
#include <utility>


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

    /* Compute a query plan.  Considers only left-deep plans for now. */
    for (auto &Q : batch) {
        JoinMatrix M(Q);
        std::cerr << "join matrix for query " << Q << '\n';
        M.print(std::cerr, Q);
        std::cerr << std::endl;

        /* Find the cheapest join to start with. */
        QueryDescription::Join start = Q.joins[0];
        std::size_t min_cardinality = std::numeric_limits<std::size_t>::max();
        for (auto J : Q.joins) {
            const auto lhs = J.lhs.relation;
            const auto rhs = J.rhs.relation;

            Relation &relation_lhs = catalog[Q.relations[lhs]];
            Relation &relation_rhs = catalog[Q.relations[rhs]];

            const std::size_t cardinality = relation_lhs.rows() * relation_rhs.rows() * M.at(lhs, rhs);
            std::cerr << "- estimated cardinality: r" << Q.relations[lhs] << " |X| r" << Q.relations[rhs] << " => "
                      << relation_lhs.rows() << " * " << relation_rhs.rows() << " * " << M.at(lhs, rhs) << " = "
                      << (cardinality / 1e6) << " Mio\n";

            if (cardinality < min_cardinality) {
                min_cardinality = cardinality;
                start = J;
            }
        }
        std::cerr << "start with join " << start << '\n';

        bool *used_relations = new bool[Q.relations.size()]();
        used_relations[start.lhs.relation] = true;
        used_relations[start.rhs.relation] = true;

        /* Consecutively search for the next cheapest join. */
    }
}
