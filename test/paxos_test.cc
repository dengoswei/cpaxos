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
            PaxosCallBack callback;
            callback.read = [](uint64_t /* index */) -> std::unique_ptr<HardState> {

                return nullptr;
            };

            callback.write = [](const HardState& hs) -> int {
                logdebug("hs[index %" PRIu64 ", "
                         "proposed_num %" PRIu64 ", promised_num %" PRIu64
                         ", accepted_num %" PRIu64 ", accepted_value %s]", 
                         hs.index(), hs.proposed_num(), 
                         hs.promised_num(), hs.accepted_num(), 
                         hs.accepted_value().c_str());

                return 0;
            };

            std::vector<Message>& vecMsg = paxos_rsp_msg_;
            callback.send = [&](const Message& rsp_msg) -> int {
                // fake => rsp nothing
                logdebug("rsp_msg[type %d, index %" PRIu64 " " 
                         "prop_num %" PRIu64 ", peer_id %d "
                         "promised_num %" PRIu64 ", "
                         "accepted_num %" PRIu64 "accepted_value %s]", 
                         static_cast<int>(rsp_msg.type()), rsp_msg.index(), 
                         rsp_msg.proposed_num(), static_cast<int>(rsp_msg.peer_id()), 
                         rsp_msg.promised_num(), rsp_msg.accepted_num(), 
                         rsp_msg.accepted_value().c_str());

                // Message msg = *rsp_msg;
                vecMsg.emplace_back(rsp_msg);
                // vecMsg.emplace_back(move(msg));

                return 0;
            };

            paxos_map_.emplace(id, unique_ptr<Paxos>{new Paxos{id, 3, callback}});
        }
    }

protected:
    std::map<uint64_t, std::unique_ptr<Paxos>> paxos_map_;
    std::vector<Message> paxos_rsp_msg_;
    RandomStrGen<10, 40> str_gen_;
};


TEST_F(PaxosTest, SimpleImplPropose)
{
    auto proposing_value = str_gen_.Next();
    Paxos* p = paxos_map_[1ull].get();
    assert(nullptr != p);

    Paxos* q = paxos_map_[2ull].get();
    assert(nullptr != q);

    int ret = 0;
    vector<Message>& vecMsg = paxos_rsp_msg_;

    uint64_t index = 0;
    tie(ret, index) = p->Propose(index, proposing_value);
    assert(0 == ret);
    assert(0 < index);

    // q: recv prop req, produce prop_rsp 
    {
        Message req_msg = vecMsg.back();
        vecMsg.clear();
        assert(MessageType::PROP == req_msg.type());
        assert(index == req_msg.index());
        assert(0 == req_msg.to_id());

        req_msg.set_to_id(q->GetSelfId());
        assert(0 != req_msg.to_id());
        ret = q->Step(req_msg);
        // ret = q->Step(req_msg, callback);
        hassert(0 == ret, "Paxos::Step ret %d", ret);
    }

    // p: recv prop rsp, produce accpt req
    {
        Message req_msg = vecMsg.back();
        vecMsg.clear();
        assert(MessageType::PROP_RSP == req_msg.type());
        assert(index == req_msg.index());

        ret = p->Step(req_msg);
        // ret = p->Step(req_msg, callback);
        hassert(0 == ret, "Paxos::Step ret %d", ret);
    }

    // q: recv accpt req, produce accpt_rsp
    {
        Message req_msg = vecMsg.back();
        vecMsg.clear();
        assert(MessageType::ACCPT == req_msg.type());
        assert(index == req_msg.index());
        assert(0 == req_msg.to_id());

        req_msg.set_to_id(q->GetSelfId());
        assert(0 != req_msg.to_id());

        ret = q->Step(req_msg);
        // ret = q->Step(req_msg, callback);
        hassert(0 == ret, "Paxos::Step ret %d", ret);
    }

    // p: recv accpt rsp, => chosen
    {
        Message req_msg = vecMsg.back();
        vecMsg.clear();
        assert(MessageType::ACCPT_RSP == req_msg.type());
        assert(index == req_msg.index());

        ret = p->Step(req_msg);
        // ret = p->Step(req_msg, callback);
        hassert(0 == ret, "Paxos::Step ret %d", ret);

        uint64_t commited_index = p->GetCommitedIndex();
        assert(commited_index == index);
    }

    // q: recv chosen req 
    {
        Message req_msg = vecMsg.back();
        vecMsg.clear();
        assert(MessageType::CHOSEN == req_msg.type());
        assert(index == req_msg.index());
        assert(0 == req_msg.to_id());

        req_msg.set_to_id(q->GetSelfId());
        assert(0 != req_msg.to_id());

        ret = q->Step(req_msg);
        // ret = q->Step(req_msg, callback);
        hassert(0 == ret, "Paxos::Step ret %d", ret);

        assert(true == vecMsg.empty());
        uint64_t commited_index = q->GetCommitedIndex();
        assert(commited_index == index);
    }
}



