#include "utils.h"
#include "paxos.h"
#include "paxos_instance.h"
#include "gtest/gtest.h"
#include "paxos.pb.h"


using namespace paxos;
using namespace std;


class PaxosTest : public ::testing::Test {

protected:
    virtual void SetUp() override 
    {
        for (uint64_t id = 1; id <= 3; ++id)
        {
            paxos_map_.emplace(id, unique_ptr<Paxos>{new Paxos{id, 3}});
        }
    }

protected:
    std::map<uint64_t, std::unique_ptr<Paxos>> paxos_map_;
    RandomStrGen<10, 40> str_gen_;
};


TEST_F(PaxosTest, SimpleImplPropose)
{
    auto proposing_value = str_gen_.Next();
    Paxos* p = paxos_map_[1ull].get();
    assert(nullptr != p);

    Paxos* q = paxos_map_[2ull].get();
    assert(nullptr != q);

    vector<tuple<uint64_t, Message>> vecMsg;
    auto callback = [&](
            const unique_ptr<HardState>& hs, 
            const unique_ptr<Message>& rsp_msg) {

        // fake => store nothing
        if (nullptr != hs) {
            logdebug("hs[index %" PRIu64 ", "
                     "proposed_num %" PRIu64 ", promised_num %" PRIu64
                     ", accepted_num %" PRIu64 ", accepted_value %s]", 
                     hs->index(), hs->proposed_num(), 
                     hs->promised_num(), hs->accepted_num(), 
                     hs->accepted_value().c_str());
        }

        // fake => rsp nothing
        if (nullptr != rsp_msg) {
            logdebug("rsp_msg[type %d, index %" PRIu64 " " 
                     "prop_num %" PRIu64 ", peer_id %d "
                     "promised_num %" PRIu64 ", "
                     "accepted_num %" PRIu64 "accepted_value %s]", 
                     static_cast<int>(rsp_msg->type()), rsp_msg->index(), 
                     rsp_msg->proposed_num(), static_cast<int>(rsp_msg->peer_id()), 
                     rsp_msg->promised_num(), rsp_msg->accepted_num(), 
                     rsp_msg->accepted_value().c_str());

            Message msg = *rsp_msg;
            vecMsg.emplace_back(make_tuple(rsp_msg->index(), msg));
        }

        return 0;
    };

    int ret = 0;
    uint64_t index = 0;
    tie(ret, index) = p->Propose(proposing_value, callback);
    hassert(0 == ret, "ret %d", ret);
    assert(0 < index);

    // q: recv prop req, produce prop_rsp 
    {
        uint64_t req_index = 0;
        Message req_msg;
        tie(req_index, req_msg) = vecMsg.back();
        vecMsg.clear();
        assert(MessageType::PROP == req_msg.type());
        assert(index == req_index);
        assert(0 == req_msg.to_id());

        req_msg.set_to_id(q->GetSelfId());
        assert(0 != req_msg.to_id());
        ret = q->Step(req_index, req_msg, callback);
        hassert(0 == ret, "Paxos::Step ret %d", ret);
    }

    // p: recv prop rsp, produce accpt req
    {
        uint64_t req_index = 0;
        Message req_msg;
        tie(req_index, req_msg) = vecMsg.back();
        vecMsg.clear();
        assert(MessageType::PROP_RSP == req_msg.type());
        assert(index == req_index);

        ret = p->Step(req_index, req_msg, callback);
        hassert(0 == ret, "Paxos::Step ret %d", ret);
    }

    // q: recv accpt req, produce accpt_rsp
    {
        uint64_t req_index = 0;
        Message req_msg;
        tie(req_index, req_msg) = vecMsg.back();
        vecMsg.clear();
        assert(MessageType::ACCPT == req_msg.type());
        assert(index == req_index);
        assert(0 == req_msg.to_id());

        req_msg.set_to_id(q->GetSelfId());
        assert(0 != req_msg.to_id());
        ret = q->Step(req_index, req_msg, callback);
        hassert(0 == ret, "Paxos::Step ret %d", ret);
    }

    // p: recv accpt rsp, => chosen
    {
        uint64_t req_index = 0;
        Message req_msg;
        tie(req_index, req_msg) = vecMsg.back();
        vecMsg.clear();
        assert(MessageType::ACCPT_RSP == req_msg.type());
        assert(index == req_index);

        ret = p->Step(req_index, req_msg, callback);
        hassert(0 == ret, "Paxos::Step ret %d", ret);

        uint64_t commited_index = p->GetCommitedIndex();
        assert(commited_index == index);
    }

    // q: recv chosen req 
    {
        uint64_t req_index = 0;
        Message req_msg;
        tie(req_index, req_msg) = vecMsg.back();
        vecMsg.clear();
        assert(MessageType::CHOSEN == req_msg.type());
        assert(index == req_index);
        assert(0 == req_msg.to_id());

        req_msg.set_to_id(q->GetSelfId());
        assert(0 != req_msg.to_id());

        ret = q->Step(req_index, req_msg, callback);
        hassert(0 == ret, "Paxos::Step ret %d", ret);

        assert(true == vecMsg.empty());
        uint64_t commited_index = q->GetCommitedIndex();
        assert(commited_index == index);
    }

}



