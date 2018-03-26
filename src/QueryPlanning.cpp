#include "QueryPlanning.hpp"

#include <iomanip>


JoinMatrix::JoinMatrix(QueryDescription &Q)
{
    const std::size_t num_relations = Q.relations.size();
    const std::size_t size_join_matrix = num_relations * (num_relations + 1) / 2;
    join_matrix_ = new float[size_join_matrix];
    std::fill_n(join_matrix_, size_join_matrix, 1.f);

    for (auto J : Q.joins) {
        auto lhs = J.lhs.relation;
        auto rhs = J.rhs.relation;
        at(lhs, rhs) *= .42f; /* TODO sample joins */
    }
}

void JoinMatrix::print(std::ostream &out, const QueryDescription &Q)
{
    out << std::fixed << std::showpoint << std::setprecision(3) << std::left;
    for (std::size_t i = 0; i != Q.relations.size(); ++i) {
        out << 'r' << std::setw(2) << Q.relations[i];
        for (std::size_t j = 0; j <= i; ++j)
            out << " | " << std::setw(3) << at(i, j);
        out << '\n';
    }
    out << "   ";
    for (auto r : Q.relations)
        out << " | r" << std::setw(4) << r;
    out << '\n';
}
