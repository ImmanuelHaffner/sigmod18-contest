#pragma once

#include "Catalog.hpp"
#include "QueryDescription.hpp"
#include <cstdint>
#include <vector>


/** This class implements query planning of whole batches (MQO) and concurrent execution of the queries. */
class Scheduler
{
    private:
    Catalog &catalog_;
    std::size_t num_threads_; ///< number of available threads

    public:
    Scheduler(Catalog &catalog, std::size_t num_threads) : catalog_(catalog), num_threads_(num_threads) { }

    /** Executes a batch of queries. */
    void execute(std::vector<QueryDescription> &batch);
};
