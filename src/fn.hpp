#pragma once

#include <cstdint>
#include <cstdlib>
#include <cstring>


template<typename T>
static T ceil_to_pow2(T n)
{
    n--;
    n |= n >> 1;
    n |= n >> 2;
    n |= n >> 4;
    if constexpr (sizeof(T) >= 2) {
        n |= n >> 8;
        if constexpr (sizeof(T) >= 4) {
            n |= n >> 16;
            if constexpr (sizeof(T) >= 8) {
                n |= n >> 32;
            }
        }
    }
    n++;
    return n;
}

inline bool streq(const char *first, const char *second) { return 0 == strcmp(first, second); }

struct StrHash
{
    std::size_t operator()(const char *c_str) const {
        /* FNV-1a 64 bit */
        std::size_t hash = 0xcbf29ce484222325;
        char c;
        while ((c = *c_str++)) {
            hash = hash ^ c;
            hash = hash * 1099511628211;
        }
        return hash;
    }
};

struct StrEqual
{
    bool operator()(const char *first, const char *second) const { return streq(first, second); }
};

struct StrEqualWithNull
{
    bool operator()(const char *first, const char *second) const {
        return first == second or (first != nullptr and second != nullptr and streq(first, second));
    }
};

struct Murmur3
{
    uint32_t operator()(uint32_t v) const {
        v ^= v >> 16;
        v *= 0x85ebca6b;
        v ^= v >> 13;
        v *= 0xc2b2ae35;
        v ^= v >> 16;
        return v;
    }
};

struct Murmur3_64
{
    uint64_t operator()(uint64_t v) const {
        v ^= v >> 33;
        v *= 0xff51afd7ed558ccd;
        v ^= v >> 33;
        v *= 0xc4ceb9fe1a85ec53;
        v ^= v >> 33;
        return v;
    }
};

/** Computes n over k. */
inline unsigned binomial(unsigned n, unsigned k)
{
    unsigned c = 1;
    k = std::min(k, n - k); /* take advantage of symmetry */
    for (unsigned i = 1; i <= k; i++, n--)
        c = c / i * n + c % i * n / i;  /* split c*n/i into (c/i*i + c%i)*n/i */
    return c;
}
