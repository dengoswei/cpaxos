#include "gtest/gtest.h"
#include "random_utils.h"
#include "id_utils.h"

using cutils::RandomStrGen;
using cutils::PropNumGen;

TEST(TestPropNumGen, SimpleTest)
{
    PropNumGen prop_num_gen(2);
    ASSERT_EQ(2, prop_num_gen.Get());
    ASSERT_EQ((1 << 16) + 2, prop_num_gen.Next(0));
    ASSERT_EQ((3 << 16) + 2, prop_num_gen.Next((3 << 16) + 1));
    ASSERT_EQ((5 << 16) + 2, prop_num_gen.Next((4 << 16) + 3));
}

TEST(TestRandom, SimpleTest)
{
    RandomStrGen<10, 20> sgen;
    auto s = sgen.Next();
    ASSERT_LE(10, s.size());
    ASSERT_GE(20, s.size());
}

