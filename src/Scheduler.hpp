#pragma once

#include "Catalog.hpp"
#include "QueryDescription.hpp"
#include <cstdint>
#include <vector>


/** This class implements query planning of whole batches (MQO) and concurrent execution of the queries. */
class Scheduler
{
    public:
    Catalog &catalog;
    std::size_t num_threads; ///< number of available threads

    public:
    Scheduler(Catalog &catalog, std::size_t num_threads) : catalog(catalog), num_threads(num_threads) { }

    /** Executes a batch of queries. */
    void execute(std::vector<QueryDescription> &batch);
};
