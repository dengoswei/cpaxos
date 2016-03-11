#include <string>
#include "paxos.pb.h"
#include "utils.h"
#include "gtest/gtest.h"


using namespace paxos;


TEST(MessageTest, SimpleTest)
{
    Message msg;

    assert(false == msg.has_promised_num());
    assert(false == msg.has_accepted_value());

    {
        auto promised_num = msg.promised_num();
        assert(0 == promised_num);
        assert(false == msg.has_promised_num());
    }

    {
        auto accepted_value = msg.accepted_value();
        assert(false == msg.has_accepted_value());
    }

    {
        auto accepted_value = msg.mutable_accepted_value();
        assert(nullptr != accepted_value);
        assert(true == msg.has_accepted_value());
    }
}
