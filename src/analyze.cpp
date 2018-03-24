#include "analyze.hpp"

#include "assure.hpp"
#include <algorithm>
#include <iostream>


void analyze(const std::vector<QueryDescription> &batch)
{
    std::size_t max_relation_id = 0;
    for (auto &Q : batch) {
        for (auto r : Q.relations)
            max_relation_id = std::max(max_relation_id, r);
    }

    /* Count the occurance of each relation. */
    unsigned *relation_occurance_count = new unsigned[max_relation_id + 1]();
    for (auto &Q : batch) {
        for (auto r : Q.relations)
            ++relation_occurance_count[r];
    }
    for (std::size_t i = 0; i != max_relation_id; ++i) {
        unsigned c = relation_occurance_count[i];
        if (c)
            std::cerr << 'r' << i << ": " << c << '\n';
    }
    delete[] relation_occurance_count;

    /* Count the frequency of join pairs. */
    std::size_t size_join_matrix = (max_relation_id + 1) * (max_relation_id + 2) / 2;
    unsigned *join_matrix = new unsigned[size_join_matrix]();
    for (auto &Q : batch) {
        for (auto J : Q.joins) {
            std::size_t lhs = Q.relations[J.lhs.relation];
            std::size_t rhs = Q.relations[J.rhs.relation];
            if (lhs < rhs) std::swap(lhs, rhs);
            assure(lhs >= rhs);
            join_matrix[lhs * (lhs + 1) / 2 + rhs]++;
        }
    }
    {
        std::size_t x = 0, y = 0;
        for (std::size_t i = 0; i != size_join_matrix; ++i) {
            if (join_matrix[i])
                std::cerr << 'r' << x << "|X|r" << y << ": " << join_matrix[i] << '\n';
            if (x == y) {
                y = 0;
                ++x;
            } else
                ++y;
        }
    }
    delete[] join_matrix;
}
