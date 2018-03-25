#include "catch.hpp"

#include "Index.hpp"


TEST_CASE("IndexTest", "[unit]")
{
    Index I;
}

TEST_CASE("IndexTest/put", "[unit]")
{
    Index I;
    auto f0 = I.add_field();
    auto f1 = I.add_field();
    REQUIRE(f0 == 0);
    REQUIRE(f1 == 1);

    {
        auto res = I.put(42);
        REQUIRE(res.second);
        auto v = res.first->second;
        REQUIRE(v);
        v[f0] = 1;
        v[f1] = 1337;
    }
    {
        auto res = I.put(13);
        REQUIRE(res.second);
        auto v = res.first->second;
        REQUIRE(v);
        v[f0] = 2;
        v[f1] = 0xC0FFEE;
    }

    {
        auto v = I[42];
        REQUIRE(v[f0] == 1);
        REQUIRE(v[f1] == 1337);
    }
    {
        auto v = I[13];
        REQUIRE(v[f0] == 2);
        REQUIRE(v[f1] == 0xC0FFEE);
    }
}
