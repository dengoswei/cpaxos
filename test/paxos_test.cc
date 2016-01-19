#include "gtest/gtest.h"
#include "paxos.pb.h"
#include "test_helper.h"
#include "paxos.h"


using namespace std;
using namespace paxos;
using namespace test;

TEST(PaxosTest, SimpleConstruct)
{
    auto logid = LOGID;
    auto group_ids = GROUP_IDS;

    auto selfid = 1ull;
    assert(group_ids.end() != group_ids.find(selfid));

    SendHelper sender{0};
    map<uint64_t, unique_ptr<StorageHelper>> map_storage;
    map<uint64_t, unique_ptr<Paxos>> map_paxos;

    tie(map_storage, map_paxos) = 
        build_paxos(logid, group_ids, sender, 0);
    assert(map_paxos.size() == group_ids.size());
    assert(map_storage.size() == group_ids.size());

    auto& paxos = map_paxos[selfid];
    assert(nullptr != paxos);
    assert(0ull == paxos->GetCommitedIndex());
    assert(0ull == paxos->GetMaxIndex());
}

TEST(PaxosTest, SimplePropose)
{
    auto logid = LOGID;
    auto group_ids = GROUP_IDS;

    auto selfid = 1ull;
    assert(group_ids.end() != group_ids.find(selfid));

    SendHelper sender{0};
    map<uint64_t, unique_ptr<StorageHelper>> map_storage;
    map<uint64_t, unique_ptr<Paxos>> map_paxos;

    tie(map_storage, map_paxos) = 
        build_paxos(logid, group_ids, sender, 0);
    assert(map_paxos.size() == group_ids.size());
    assert(map_storage.size() == group_ids.size());

    auto& paxos = map_paxos[selfid];
    assert(nullptr != paxos);

    string prop_value;
    {
        auto prop_msg = buildMsgProp(logid, selfid, 1ull);
        assert(nullptr != prop_msg);
        prop_value = prop_msg->accepted_value();
    }

    auto err = paxos::ErrorCode::OK;
    auto prop_index = 0ull;
    tie(err, prop_index) = paxos->Propose(0ull, prop_value);
    assert(paxos::ErrorCode::OK == err);
    assert(0ull < prop_index);

    // msg prop
    assert(false == sender.empty());
    auto apply_count = sender.apply(map_paxos);
    assert(size_t{2} == apply_count);

    // msg prop rsp
    assert(false == sender.empty());
    apply_count = sender.apply(map_paxos);
    assert(size_t{2} == apply_count);

    // msg accpt
    assert(false == sender.empty());
    apply_count = sender.apply(map_paxos);
    assert(size_t{2} == apply_count);

    // msg accpt rsp
    assert(false == sender.empty());
    apply_count = sender.apply(map_paxos);
    assert(size_t{2} == apply_count);

    // msg chosen
    assert(false == sender.empty());
    apply_count = sender.apply(map_paxos);
    assert(size_t{2} < apply_count);

    // => all mark as chosen now
    for (auto& id_paxos : map_paxos) {
        auto& peer_paxos = id_paxos.second;
        assert(nullptr != peer_paxos);

        assert(prop_index == peer_paxos->GetMaxIndex());
        assert(prop_index == peer_paxos->GetCommitedIndex());
        peer_paxos->Wait(prop_index);

        // check value
        {
            auto& storage = map_storage[id_paxos.first];
            assert(nullptr != storage);

            auto hs = storage->read(logid, prop_index);
            assert(nullptr != hs);
            assert(prop_index == hs->index());
            assert(0ull < hs->proposed_num());
            assert(0ull < hs->promised_num());
            assert(0ull < hs->accepted_num());
            assert(prop_value == hs->accepted_value());
            assert(logid == hs->logid());
            assert(0 < hs->seq());
        }
    }
}






