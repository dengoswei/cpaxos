#include "utils.h"
#include "paxos.h"
#include "gtest/gtest.h"


using namespace paxos;


class PaxosTest : public ::testing::Test {

protecte:
    virtual void SetUp() override 
    {
        for (uint64_t id = 1; id <= 3; ++id)
        {
            paxos_map_.emplace(id, new Paxos{id});
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

    vector<tuple<int, Message>> vecMsg;
    auto callback = [](
            uint64_t index, 
            const unique_ptr<proto::HardState>& hs, 
            const unique_ptr<Message>& rsp_msg) {

        // fake => store nothing
        if (nullptr != hs) {
            logdebug("index %" PRIu64 " hs[index %" PRIu64 ", "
                     "proposed_num %" PRIu64 ", promised_num %" PRIu64
                     ", accepted_num %"PRIu64 ", accepted_value %s]", 
                     index, hs->index(), hs->proposed_num(), 
                     hs->promised_num(), hs->accepted_num(), 
                     hs->accepted_value().c_str());
        }

        // fake => rsp nothing
        if (nullptr != rsp_msg) {
            logdebug("index %" PRIu64 " rsp_msg[type %d, "
                     "prop_num %" PRIu64 ", peer_id %d "
                     "promised_num %" PRIu64 ", "
                     "accepted_num %" PRIu64 "accepted_value %s]", 
                     index, static_cast<int>(rsp_msg.type), 
                     rsp_msg.prop_num, static_cast<int>(rsp_msg.peer_id), 
                     rsp_msg.promised_num, rsp_msg.accepted_num, 
                     rsp_msg.accepted_value.c_str());

            Message msg = *rsp_msg;
            vecMsg.emplace_back(make_tuple(index, msg));
        }

        return 0;
    };

    int ret = 0;
    uint64_t index = 0;
    tie(ret, index) = p->Propose(proposing_value, callback);
    hassert(0 == ret, "ret %d", ret);
    assert(0 < index);

    {
        uint64_t req_index = 0;
        uint64_t req_msg;
        tie(req_index, req_msg) = vecMsg.back();
        vecMsg.clear();
        assert(MessageType::PROP == req_msg.type);
        ret = q->Step(req_index, req_msg, callback);
        hassert(0 == ret, "Paxos::Step ret %d", ret);


    }


}



