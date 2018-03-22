#pragma once

#include <cstdlib>
#include <iostream>


/* This function implements `assure()` similar to `assert()`, yet allows to provide an optional message. */
inline void _assure(const bool pred, const char *filename, const unsigned line, const char *predstr, const char *msg)
{
    if (pred) return;

    std::cout.flush();
    std::cerr << filename << ':' << line << ": Condition  (" << predstr << ")  failed";
    if (msg)
        std::cout << ": \"" << msg << '"';
    std::cout << '.' << std::endl;
    abort();
}
#ifndef NDEBUG
#define _ASSURE2(PRED, MSG) _assure((PRED), __FILE__, __LINE__, #PRED, MSG)
#else
#define _ASSURE2(PRED, MSG)
#endif
#define _ASSURE1(PRED) _ASSURE2(PRED, nullptr)
#define _GET_ASSURE(_1, _2, NAME, ...) NAME
#define assure(...) _GET_ASSURE(__VA_ARGS__, _ASSURE2, _ASSURE1, XXX)(__VA_ARGS__)


[[noreturn]] inline void _abort(const char *filename, const unsigned line, const char *msg)
{
    std::cout.flush();
    std::cerr << filename << ':' << line << ": " << msg << std::endl;
    abort();
}

#define unreachable(MSG) _abort(__FILE__, __LINE__, (MSG))


template<typename T>
T * _nonnull(T *arg, const char *filename, const unsigned line, const char *argstr)
{
    if (not arg) {
        std::cout.flush();
        std::cerr << filename << ':' << line << ": " << argstr << " was NULL" << std::endl;
        abort();
    }
    return arg;
}
#define nonnull(ARG) _nonnull((ARG), __FILE__, __LINE__, #ARG)
