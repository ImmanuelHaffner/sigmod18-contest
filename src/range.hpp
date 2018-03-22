#pragma once


/**
 * This class is a lightweight proxy to build ranges.  This simplifies for-loops where
 *   for (auto it = Obj.some_begin(), end = Obj.some_end(); it != end; ++it) { ... }
 * can be replaced by
 *   for (auto entry : Obj.some()) { ... }
 * and `Obj.some()` returns a `range` object.
 */
template<typename Iterator>
class range
{
    private:
    Iterator begin_;
    Iterator end_;

    public:
    range(Iterator begin, Iterator end) : begin_(begin), end_(end) { }

    Iterator begin() { return begin_; }
    Iterator end() { return end_; }
};

template<typename Iterator>
range<Iterator> make_range(Iterator begin, Iterator end) { return range<Iterator>(begin, end); }
