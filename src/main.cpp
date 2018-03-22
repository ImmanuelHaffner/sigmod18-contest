#include <iostream>

#include "Catalog.hpp"
#include "Relation.hpp"
#include <string>
#include <vector>


int main()
{
    Catalog C;

    /*-- Read the input relations. -----------------------------------------------------------------------------------*/
    std::string buf;
    for (;;) {
        std::cin >> buf;
        if (not std::cin.good())
            break;
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
}
