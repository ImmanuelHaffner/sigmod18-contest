#pragma once

#include "HashTable.hpp"


class Index
{
    private:
    using table_type = hash_map<uint64_t, uint64_t*>;

    public:
    using size_type = typename table_type::size_type;

    private:
    table_type table_;
    size_type num_fields_ = 0;

    public:
    Index(uint64_t empty = 0UL, size_type capacity = 1024) : table_(empty, capacity) { }
    ~Index() {
        for (auto entry : table_)
            delete[] entry.second;
    }

    /** Adds a field to the value part of a key-value-pair.  Returns the offset of the field inside the value part. */
    size_type add_field() { return num_fields_++; }
    /** Return the number of fields in the value part. */
    size_type num_fields() const { return num_fields_; }


    std::pair<typename table_type::iterator, bool> put(uint64_t key) {
        /* TODO use insert_hint to improve performance */
        auto it = table_.find(key);
        if (it != table_.end()) return {it, false};
        return table_.insert({key, get_fresh_value()});
    }

    uint64_t * operator[](uint64_t key) {
        uint64_t *&value = table_[key];
        if (not value) value = get_fresh_value();
        return value;
    }

    private:
    uint64_t * get_fresh_value() { return new uint64_t[num_fields_]; }
};
