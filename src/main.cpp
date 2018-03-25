#include <iostream>

#include "analyze.hpp"
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

    /*-- Read the workload batches. ----------------------------------------------------------------------------------*/
    std::vector<QueryDescription> batch;
    for (;;) {
        auto Q = QueryDescription::Parse(std::cin);
        if (not std::cin.good())
            break;
        batch.push_back(Q);
        if (std::cin.peek() == 'F')
            break;
    }

    for (auto &Q : batch)
        std::cerr << Q << '\n';

    analyze(batch);

    Scheduler S(C, 1);
    S.execute(batch);
}
