#pragma once

#include <cstdint>
#include <iostream>
#include <utility>
#include <vector>

/** This class represents one query of a batch from the workload. */
class QueryDescription
{
    public:
    struct Attr
    {
        std::size_t relation;
        std::size_t attribute;
        bool operator==(Attr other) const { return relation == other.relation and attribute == other.attribute; }
        bool operator!=(Attr other) const { return not operator==(other); }
        friend std::ostream & operator<<(std::ostream &out, Attr A) { return out << A.relation << '.' << A.attribute; }
    };

    struct Join
    {
        Attr lhs;
        Attr rhs;

        Join(Attr lhs, Attr rhs) {
            using std::swap;
            if (lhs.relation > rhs.relation)
                swap(lhs, rhs);
            this->lhs = lhs;
            this->rhs = rhs;
        }

        bool operator==(const Join &other) const { return lhs == other.lhs and rhs == other.rhs; }
        bool operator!=(const Join &other) const { return not operator==(other); }

        friend std::ostream & operator<<(std::ostream &out, const Join &J) { return out << J.lhs << '=' << J.rhs; }
    };

    struct Filter
    {
        Attr lhs;
        uint64_t value;
        char op;
        friend std::ostream & operator<<(std::ostream &out, const Filter &F) { return out << F.lhs << F.op << F.value; }
    };

    std::vector<std::size_t> relations;
    std::vector<Join> joins;
    std::vector<Filter> filters;
    std::vector<Attr> projections;

    static QueryDescription Parse(std::istream &in);
    friend std::ostream & operator<<(std::ostream &out, const QueryDescription &Q);
};
