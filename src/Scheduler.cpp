#include "Scheduler.hpp"

#include "QueryPlanning.hpp"
#include <algorithm>
#include <iomanip>
#include <iostream>
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

    /* Compute a query plan. */
#if 0
    for (auto &Q : batch) {
        const auto num_relations = Q.relations.size();
        const std::size_t size_join_matrix = num_relations * (num_relations + 1) / 2;
        float *join_matrix = new float[size_join_matrix];
        std::fill_n(join_matrix, size_join_matrix, 1.f);

        auto pos = [](std::size_t lhs, std::size_t rhs) {
            if (lhs < rhs) std::swap(lhs, rhs);
            return lhs * (lhs + 1) / 2 + rhs;
        };

        for (auto J : Q.joins) {
            auto lhs = J.lhs.relation;
            auto rhs = J.rhs.relation;
            join_matrix[pos(lhs, rhs)] *= .9f;
        }

#ifndef NDEBUG
        std::cerr << "Join Matrix for query " << Q << '\n';
        for (std::size_t i = 0; i != num_relations; ++i) {
            std::cerr << 'r' << std::setw(2) << Q.relations[i];
            for (std::size_t j = 0; j <= i; ++j)
                std::cerr << " | " << std::setw(3) << join_matrix[pos(i, j)];
            std::cerr << '\n';
        }
        std::cerr << "   ";
        for (auto r : Q.relations)
            std::cerr << " | r" << std::setw(2) << r;
        std::cerr << '\n' << std::endl;
#endif

        delete[] join_matrix;
    }
#else
    for (auto &Q : batch) {
        JoinMatrix M(Q);
        std::cerr << "join matrix for query " << Q << '\n';
        M.print(std::cerr, Q);
        std::cerr << std::endl;
    }
#endif
}
