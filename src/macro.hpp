/*--- macros.hpp -------------------------------------------------------------------------------------------------------
 *
 * This file provides convenience macros.
 *
 *--------------------------------------------------------------------------------------------------------------------*/


#pragma once

#include <iostream>

#define __ID(X) X
#define __STR(X) __STR_(X)
#define __STR_(X) #X

#define DECLARE_ENUM(LIST) \
    enum LIST { \
        LIST(__ID) \
    }
#define ENUM_TO_STR(LIST) LIST(__STR)

#define ARR_SIZE(ARR) (sizeof(ARR) / sizeof(*(ARR)))

#define DUMP \
    void dump(std::ostream &out) const __attribute__((noinline)) { out << *this << std::endl; } \
    void dump() const __attribute__((noinline)) { dump(std::cerr); }
