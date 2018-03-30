#include <iostream>

#include "Catalog.hpp"
#include "Index.hpp"
#include "QueryDescription.hpp"
#include "Relation.hpp"
#include "Scheduler.hpp"
#include <algorithm>
#include <err.h>
#include <string>
#include <vector>


int main()
{
    /*-- Read the input relations. -----------------------------------------------------------------------------------*/
    Catalog &C = Catalog::Get();
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
    std::cerr << "loaded relations:\n";
    for (auto &r : C.relations())
        std::cerr << "  * " << *r << '\n';
#endif

    /*-- Initialize the scheduler.  ----------------------------------------------------------------------------------*/
    Scheduler S(C, 1);

    /*-- Read the workload batches. ----------------------------------------------------------------------------------*/
    std::vector<QueryDescription> batch;
    for (;;) {
        auto Q = QueryDescription::Parse(std::cin);
        if (not std::cin.good())
            break;
        batch.push_back(Q);
        if (std::cin.peek() == 'F') {
            std::cin.get();
#ifndef NDEBUG
            std::cerr << "\n###  BATCH  ####################################################################\n";
#endif
            S.execute(batch);
            batch.clear();
        }
        if (std::cin.peek() == '\n') { // end of workload
            std::cin.get();
            std::cin.peek();
            if (not std::cin.good())
                break;
        }
    }
}
