#pragma once

#include <cstdint>
#include <iostream>
#include <vector>

/** This class represents one query of a batch from the workload. */
class QueryDescription
{
    public:
    struct Attr
    {
        std::size_t relation;
        std::size_t attribute;
        friend std::ostream & operator<<(std::ostream &out, Attr A) {
            return out << A.relation << '.' << A.attribute;
        }
    };

    struct Join
    {
        Attr lhs;
        Attr rhs;
        friend std::ostream & operator<<(std::ostream &out, const Join &J) { return out << J.lhs << '=' << J.rhs; }
    };

    struct Filter
    {
        Attr lhs;
        uint64_t value;
        char op;
        friend std::ostream & operator<<(std::ostream &out, const Filter &F) { return out << F.lhs << F.op << F.value; }
    };

    private:
    std::vector<std::size_t> relations_;
    std::vector<Join> joins_;
    std::vector<Filter> filters_;
    std::vector<Attr> projections_;

    public:
    static QueryDescription Parse(std::istream &in);

    friend std::ostream & operator<<(std::ostream &out, const QueryDescription &Q);
};
