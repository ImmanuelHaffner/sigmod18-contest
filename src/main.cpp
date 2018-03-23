#include <iostream>

#include "Catalog.hpp"
#include "QueryDescription.hpp"
#include "Relation.hpp"
#include <algorithm>
#include <err.h>
#include <string>
#include <vector>


int main()
{
#if 0
    /*-- Modify the locale to make '|' a delimiter character. --------------------------------------------------------*/
    class my_ctype : public std::ctype<char>
    {
        mask table_[table_size];

        public:
        my_ctype(size_t refs = 0) : std::ctype<char>(table_, false, refs)
        {
            std::copy_n(classic_table(), table_size, table_);
            table_[std::size_t('|')] = (mask) space;
        }
    };
    std::locale loc(std::locale::classic(), new my_ctype());
    std::cin.imbue(loc);
#endif

    /*-- Read the input relations. -----------------------------------------------------------------------------------*/
    Catalog C;
    std::string buf;
    for (;;) {
        std::cin >> buf;
        if (not std::cin.good())
            errx(EXIT_FAILURE, "Unexpected end of input.");
        if (buf == "Done")
            break;
        C.add_relation(buf);
    }
#ifndef NDEBUG
    std::cerr << "loaded relations:";
    for (auto &r : C.relations())
        std::cerr << ' ' << *r << ',';
    std::cerr << '\n';
#endif

    for (;;) {
        auto Q = QueryDescription::Parse(std::cin);
        if (not std::cin.good())
            break;
        std::cerr << Q << '\n';
        if (std::cin.peek() == 'F')
            break;
    }
}
