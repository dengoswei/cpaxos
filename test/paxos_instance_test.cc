#include "utils.h"
#include "paxos_instance.h"
#include "gtest/gtest.h"
#include "paxos.pb.h"
#include "gsl.h"


using namespace paxos;

class PaxosInstanceTest : public ::testing::Test {

protected:
    RandomStrGen<10, 40> str_gen_;
};

TEST_F(PaxosInstanceTest, SimpleImplPropose)
{
    const int major_cnt = 2; // group_size = 3
    const uint8_t selfid = 2;
    PaxosInstanceImpl ins(major_cnt, prop_num_compose(selfid, 0));
    ASSERT_EQ(ins.getPropState(), PropState::NIL);

    // begin proposed
    auto proposing_value = str_gen_.Next();
    {
        Message msg;
        msg.set_type(MessageType::BEGIN_PROP);
        msg.set_accepted_value(proposing_value);
    
        auto rsp_msg_type = ins.step(msg);
        assert(MessageType::PROP == rsp_msg_type);
    }
    ASSERT_EQ(ins.getPropState(), PropState::WAIT_PREPARE);
    // self promised
    ASSERT_EQ(ins.getProposeNum(), ins.getPromisedNum());

    Message msg;
    msg.set_type(MessageType::PROP_RSP);
    msg.set_proposed_num(ins.getProposeNum());
    msg.set_promised_num(ins.getProposeNum());

    msg.set_peer_id(1);
    assert(msg.peer_id() != static_cast<uint64_t>(selfid));
    auto rsp_msg_type = ins.step(msg);
    ASSERT_EQ(MessageType::ACCPT, rsp_msg_type);

    ASSERT_EQ(PropState::WAIT_ACCEPT, ins.getPropState());
    // self accepted
    ASSERT_EQ(ins.getProposeNum(), ins.getAcceptedNum());
    ASSERT_EQ(proposing_value, ins.getAcceptedValue());

    msg.set_type(MessageType::ACCPT_RSP);
    msg.set_accepted_num(msg.promised_num());
    rsp_msg_type = ins.step(msg);
    ASSERT_EQ(MessageType::CHOSEN, rsp_msg_type);
    ASSERT_EQ(PropState::CHOSEN, ins.getPropState());
}

TEST_F(PaxosInstanceTest, SimplePropose)
{
    const int major_cnt = 2;
    const uint8_t selfid = 2;

    PaxosInstance ins(major_cnt, prop_num_compose(selfid, 0));

    auto proposing_value = str_gen_.Next();
    {
        Message msg;
        msg.set_type(MessageType::BEGIN_PROP);
        msg.set_accepted_value(proposing_value);
        auto rsp_msg_type = ins.Step(msg);
        assert(MessageType::PROP == rsp_msg_type);
    }

    Message msg;
    msg.set_type(MessageType::PROP_RSP);
    msg.set_proposed_num(ins.GetProposeNum());
    msg.set_promised_num(ins.GetProposeNum());
    msg.set_peer_id(1);
    auto rsp_msg_type = ins.Step(msg);
    ASSERT_EQ(MessageType::ACCPT, rsp_msg_type);

    msg.set_type(MessageType::ACCPT_RSP);
    msg.set_accepted_num(msg.promised_num());
    rsp_msg_type = ins.Step(msg);
    ASSERT_EQ(MessageType::CHOSEN, rsp_msg_type);
}


