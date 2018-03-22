#pragma once

#include "assure.hpp"
#include "fn.hpp"
#include "HashTable.hpp"


struct StringPool
{
    StringPool(std::size_t n = 1024) : table_(nullptr, n) { }

    ~StringPool() {
        for (auto elem : table_)
            free((void*) elem);
    }

    std::size_t size() const { return table_.size(); }

    const char * operator()(const char *str) {
        auto it = table_.find(str);
        if (table_.end() == it) {
            auto copy = strdup(str);
            assure(copy);
            it = table_.insert(copy).first;
            assure(streq(*it, str));
        }
        return *it;
    }

    private:
    using table_t = hash_set<const char *, StrHash, StrEqualWithNull>;
    table_t table_;

    public:
    static StringPool Global;
};
