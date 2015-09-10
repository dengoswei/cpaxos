#include "utils.h"
#include "gtest/gtest.h"

using namespace paxos;

TEST(TestPropNumGen, SimpleTest)
{
    PropNumGen prop_num_gen(2);
    ASSERT_EQ(2, prop_num_gen.Get());
    ASSERT_EQ((1 << 8) + 2, prop_num_gen.Next(0));
    ASSERT_EQ((3 << 8) + 2, prop_num_gen.Next((3 << 8) + 1));
    ASSERT_EQ((5 << 8) + 2, prop_num_gen.Next((4 << 8) + 3));
}

TEST(TestRandom, SimpleTest)
{
    RandomStrGen<10, 20> sgen;
    auto s = sgen.Next();
    ASSERT_LE(10, s.size());
    ASSERT_GE(20, s.size());
}

