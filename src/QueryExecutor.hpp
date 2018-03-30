#pragma once

#include "QueryDescription.hpp"


/** Given a `QueryDescription`, plans and executes the query.  Results are stored internally. */
class QueryExecutor
{
    public:
    const QueryDescription &query;

    private:
    uint64_t *result_;

    public:
    QueryExecutor(const QueryDescription &Q) : query(Q), result_(new uint64_t[Q.projections.size() + 1]()) { }
    ~QueryExecutor() { delete[] result_; }

    /** Plans and executes the query. */
    void execute();

    /** Returns a pointer to the check sums. */
    const uint64_t * result() const { return result_; }

    private:
    void in_place_aggregation_join(std::size_t build_relation, std::size_t build_attribute);
};
