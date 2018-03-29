#pragma once

#include "HashTable.hpp"


class Index
{
    private:
    using table_type = hash_map<uint64_t, uint64_t*>;

    public:
    using size_type = typename table_type::size_type;
    using iterator = typename table_type::iterator;
    using const_iterator = typename table_type::const_iterator;

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
    /** Adds `n` fields to the value part of a key-value-pair. */
    size_type add_fields(std::size_t n) { return num_fields_ += n; }
    /** Return the number of fields in the value part. */
    size_type num_fields() const { return num_fields_; }

    size_type size() const { return table_.size(); }
    size_type capacity() const { return table_.capacity(); }


    std::pair<iterator, bool> put(uint64_t key) {
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

    iterator find(uint64_t key) { return table_.find(key); }
    const_iterator find(uint64_t key) const { return table_.find(key); }

    iterator begin() { return table_.begin(); }
    iterator end()   { return table_.end(); }
    const_iterator begin() const { return table_.begin(); }
    const_iterator end()   const { return table_.end(); }
    const_iterator cbegin() const { return table_.begin(); }
    const_iterator cend()   const { return table_.end(); }

    private:
    uint64_t * get_fresh_value() { return new uint64_t[num_fields_](); }
};
