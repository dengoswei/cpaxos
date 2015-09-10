#include "utils.h"
#include "paxos_instance.h"
#include "gtest/gtest.h"


using namespace paxos_impl;

class PaxosInstanceTest : public ::testing::Test {

protected:
    paxos::RandomStrGen<10, 40> str_gen_;
};

TEST_F(PaxosInstanceTest, SimplePropose)
{
    const int major_cnt = 2; // group_size = 3
    const uint8_t selfid = 2;
    PaxosInstanceImpl ins(major_cnt, paxos::prop_num_compose(selfid, 0));
    ASSERT_EQ(ins.getPropState(), PropState::NIL);

    // begin proposed
    auto proposing_value = str_gen_.Next();
    int ret = ins.beginPropose(proposing_value);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(ins.getPropState(), PropState::WAIT_PREPARE);
    // self promised
    ASSERT_EQ(ins.getProposeNum(), ins.getPromisedNum());

    paxos::Message msg;
    msg.type = paxos::MessageType::PROP_RSP;
    msg.prop_num = ins.getProposeNum();
    msg.promised_num = ins.getProposeNum();

    msg.peer_id = 1;
    assert(msg.peer_id != static_cast<uint64_t>(selfid));
    auto rsp_msg_type = ins.step(msg);
    ASSERT_EQ(paxos::MessageType::ACCPT, rsp_msg_type);
    assert(paxos::MessageType::ACCPT == rsp_msg_type);

    ASSERT_EQ(PropState::WAIT_ACCEPT, ins.getPropState());
    // self accepted
    ASSERT_EQ(ins.getProposeNum(), ins.getAcceptedNum());
    ASSERT_EQ(proposing_value, ins.getAcceptedValue());

    msg.type = paxos::MessageType::ACCPT_RSP;
    msg.accepted_num = msg.promised_num;
    rsp_msg_type = ins.step(msg);
    ASSERT_EQ(paxos::MessageType::CHOSEN, rsp_msg_type);
    ASSERT_EQ(PropState::CHOSEN, ins.getPropState());
}


