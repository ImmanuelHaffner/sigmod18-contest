#include "Scheduler.hpp"

#include "QueryExecutor.hpp"
#include <algorithm>
#include <iomanip>
#include <iostream>
#include <thread>
#include <utility>


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

    std::thread *threads = new std::thread[batch.size()];
    auto queries = (QueryExecutor*) malloc(sizeof(QueryExecutor) * batch.size());

    auto fn = [](QueryExecutor *q) { q->execute(); };

    for (std::size_t i = 0; i != batch.size(); ++i) {
        const auto &Q = batch[i];
        new (&queries[i]) QueryExecutor(Q);
        new (&threads[i]) std::thread(fn, &queries[i]);
    }

    for (std::size_t i = 0; i != batch.size(); ++i) {
        threads[i].join();
        auto &Q = batch[i];
        auto &QE = queries[i];
        for (std::size_t j = 0; j != Q.projections.size(); ++j) {
            if (j != 0) std::cout << ' ';
            if (QE.result()[0] == 0)
                std::cout << "NULL";
            else
                std::cout << QE.result()[j + 1];
        }
        std::cout << '\n';

        QE.~QueryExecutor();
    }
    free(queries);
    delete[] threads;
}
