#include "paxos_instance.h"
#include "gtest/gtest.h"


using namespace paxos;

TEST(TestPraxosInstance, Dummy)
{
    // group_size: 3, prop_num: 2;
    PaxosInstance ins(3, 2);    
}


