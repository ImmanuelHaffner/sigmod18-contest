#pragma once

#include "QueryDescription.hpp"


/** Given a `QueryDescription`, plans and executes the query.  Results are stored internally. */
class QueryExecutor
{
    public:
    const QueryDescription &query;

    private:
    uint64_t *result_; ///< stores the number of join results and the check sums
    uint64_t *size_estimates_; ///< stores size estimates for all used relations (acquired by sampling filters)

    public:
    QueryExecutor(const QueryDescription &Q)
        : query(Q)
        , result_(new uint64_t[Q.projections.size() + 1]())
        , size_estimates_(new uint64_t[Q.relations.size()])
    { }
    ~QueryExecutor() {
        delete[] result_;
        delete[] size_estimates_;
    }

    /** Plans and executes the query. */
    void execute();

    /** Returns a pointer to the check sums. */
    const uint64_t * result() const { return result_; }

    private:
    /** Performs an in-place aggreation join of the query using an index on the specified relation and attribute. */
    void in_place_aggregation_join(std::size_t build_relation, std::size_t build_attribute);
    /** Computes size estimates for all relations, sampling filters to estimate selectivities. */
    void compute_size_estimates();
};
