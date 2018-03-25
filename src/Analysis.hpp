#pragma once

#include "QueryDescription.hpp"
#include <vector>


class Analysis
{
    private:
    std::vector<QueryDescription> &batch_;
    std::size_t max_relation_id_ = 0;
    std::size_t size_join_matrix_;
    unsigned *join_matrix_;

    public:
    Analysis(std::vector<QueryDescription> &batch);

    private:
    void compute_join_matrix();
};
