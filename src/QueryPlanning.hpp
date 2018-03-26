#pragma once

#include "QueryDescription.hpp"
#include <iostream>
#include <utility>


class JoinMatrix
{
    private:
    float *join_matrix_;

    public:
    explicit JoinMatrix(QueryDescription &Q);
    ~JoinMatrix() { delete[] join_matrix_; }

    float & at(std::size_t first, std::size_t second) {
        if (first < second)
            std::swap(first, second);
        return join_matrix_[first * (first + 1) / 2 + second];
    }
    const float & at(std::size_t first, std::size_t second) const {
        return const_cast<JoinMatrix*>(this)->at(first, second);
    }

    void print(std::ostream &out, const QueryDescription &Q);
};
