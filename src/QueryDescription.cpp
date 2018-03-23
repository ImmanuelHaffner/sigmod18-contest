#include "QueryDescription.hpp"

#include <err.h>


QueryDescription::Attr parse_attr(std::istream &in)
{
    std::size_t relation, attribute;
    in >> relation;
    in.get();
    in >> attribute;
    return {relation, attribute};
}

QueryDescription QueryDescription::Parse(std::istream &in)
{
    QueryDescription Q;

    int chr;

    /*-- Parse relations. --------------------------------------------------------------------------------------------*/
    for (;;) {
        if (in.peek() == '|' or not in.good())
            break;

        std::size_t relation;
        in >> relation;
        Q.relations_.push_back(relation);
    }
    if ((chr = in.get()) != '|')
        errx(EXIT_FAILURE, "corrupted workload: expected '|', got '%c'", chr);

    /*-- Parse predicates. -------------------------------------------------------------------------------------------*/
    for (;;) {
        if (in.peek() == '|' or not in.good())
            break;

        /* Parse the left hand side including the operator. */
        Attr lhs = parse_attr(in);
        char op;
        in >> op;

        /* Parse the right hand side, which is either an attribute or a constant. */
        uint64_t num;
        in >> num;
        if (in.peek() == '.') { /* parse attribute */
            in.get();
            std::size_t attribute_right;
            in >> attribute_right;
            if (in.good())
                Q.joins_.push_back({lhs, {std::size_t(num), attribute_right}});
        } else { /* we parsed a constant */
            if (in.good())
                Q.filters_.push_back({lhs, num, op});
        }

        if (in.peek() == '&')
            in.get();
    }
    if ((chr = in.get()) != '|')
        errx(EXIT_FAILURE, "corrupted workload: expected '|', got '%c'", chr);

    /*-- Parse projections. ------------------------------------------------------------------------------------------*/
    for (;;) {
        if (in.peek() == '\n') {
            in.get();
            break;
        } else if (not in.good())
            break;

        Attr P = parse_attr(in);
        if (in.good())
            Q.projections_.push_back(P);
    }

    if (not in.good())
        errx(EXIT_FAILURE, "Error parsing workload");

    return Q;
}

std::ostream & operator<<(std::ostream &out, const QueryDescription &Q) {
    /* Print relations. */
    for (std::size_t i = 0, end = Q.relations_.size(); i != end; ++i) {
        if (i != 0) out << ' ';
        out << Q.relations_[i];
    }
    out << '|';
    /* Print joins. */
    for (std::size_t i = 0, end = Q.joins_.size(); i != end; ++i) {
        if (i != 0) out << '&';
        out << Q.joins_[i];
    }
    if (not Q.joins_.empty())
        out << '&';
    for (std::size_t i = 0, end = Q.filters_.size(); i != end; ++i) {
        if (i != 0) out << '&';
        out << Q.filters_[i];
    }
    out << '|';
    for (std::size_t i = 0, end = Q.projections_.size(); i != end; ++i) {
        if (i != 0) out << ' ';
        out << Q.projections_[i];
    }

    return out;
}
