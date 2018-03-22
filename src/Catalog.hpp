#pragma once

#include "assure.hpp"
#include "range.hpp"
#include "Relation.hpp"
#include <vector>
#include <memory>


class Catalog
{
    private:
    std::vector<std::unique_ptr<Relation>> relations_;

    public:
    Relation & add_relation(std::string filename) {
        return *relations_.emplace_back(std::make_unique<Relation>(filename));
    }
    Relation & operator[](std::size_t i) { return *relations_.at(i); }
    const Relation & operator[](std::size_t i) const { return *relations_.at(i); }

    auto relations_begin() { return relations_.begin(); }
    auto relations_end()   { return relations_.end(); }
    auto relations_begin() const { return relations_.begin(); }
    auto relations_end()   const { return relations_.end(); }
    auto relations() { return make_range(relations_begin(), relations_end()); }
    auto relations() const { return make_range(relations_begin(), relations_end()); }
};
