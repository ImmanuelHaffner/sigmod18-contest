#include "Analysis.hpp"

#include "assure.hpp"


Analysis::Analysis(std::vector<QueryDescription> &batch)
    : batch_(batch)
{
    for (auto &Q : batch_) {
        for (auto r : Q.relations)
            max_relation_id_ = std::max(max_relation_id_, r);
    }
    compute_join_matrix();
}

void Analysis::compute_join_matrix()
{
    size_join_matrix_ = (max_relation_id_ + 1) * (max_relation_id_ + 2) / 2;
    unsigned *join_matrix = new unsigned[size_join_matrix_]();
    for (auto &Q : batch_) {
        for (auto J : Q.joins) {
            std::size_t lhs = Q.relations[J.lhs.relation];
            std::size_t rhs = Q.relations[J.rhs.relation];
            if (lhs < rhs) std::swap(lhs, rhs);
            assure(lhs >= rhs);
            join_matrix[lhs * (lhs + 1) / 2 + rhs]++;
        }
    }
#if 1
    {
        std::size_t x = 0, y = 0;
        for (std::size_t i = 0; i != size_join_matrix_; ++i) {
            if (join_matrix[i])
                std::cerr << 'r' << x << "|X|r" << y << ": " << join_matrix[i] << '\n';
            if (x == y) {
                y = 0;
                ++x;
            } else
                ++y;
        }
    }
#endif
}
