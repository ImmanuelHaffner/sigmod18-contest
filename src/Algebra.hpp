#pragma once

#include "Index.hpp"
#include "Relation.hpp"
#include <vector>


class HashJoinAndAggregate
{
    public:
    struct Aggregate
    {
        std::size_t relation;
        std::size_t attribute;
    };

    private:
    Index &lhs_;
    Relation &rhs_;
    std::size_t lhs_attr_;
    std::size_t rhs_attr_;
    std::vector<Aggregate> aggregates_;

    public:
    HashJoinAndAggregate(Index &lhs, lhs_attr, Relation &rhs, rhs_attr)
        : lhs_(lhs)
        , rhs_(rhs)
        , lhs_attr_(lhs_attr)
        , rhs_attr_(rhs_attr)
    { }

    /** Add an aggregate to form. */
    void aggreate(std::size_t relation, std::size_t attribute) { aggregates_.emplace_back(relation, attribute); }

    Index & eval();
};
