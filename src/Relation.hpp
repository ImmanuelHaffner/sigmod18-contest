#pragma once

#include "macro.hpp"
#include <cstdint>
#include <iostream>
#include <string>
#include <utility>

class Relation
{
    private:
    std::string filename_; ///< the file storing the relation
    const uint64_t *data_; ///< the mmap()'ed relation file
    int fd_; ///< the file descriptor of the opened relation file
    off_t size_; ///< total size of the relation file, in bytes

    public:
    Relation(std::string filename);
    Relation(const Relation&) = delete;
    Relation(Relation &&other) = delete;
    ~Relation();

    public:
    /** Returns the number of rows in this relation. */
    std::size_t rows() const { return data_[0]; }
    /** Returns the number of columns in this relation. */
    std::size_t columns() const { return data_[1]; }
    /** Returns the size of this relation in bytes. */
    std::size_t size() const { return sizeof(uint64_t) * rows() * columns(); }
    /** Returns a pointer to the beginning of the column. */
    const uint64_t * get_column(std::size_t i) const { return data_ + 2 + i * rows(); }

    friend std::ostream & operator<<(std::ostream &out, const Relation &r) {
        return out << r.filename_ << " (" << r.rows() << " rows, " << r.columns() << " columns)";
    }
    DUMP
};
