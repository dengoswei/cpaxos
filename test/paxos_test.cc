#include "gtest/gtest.h"
#include "paxos.pb.h"
#include "test_helper.h"
#include "paxos.h"


using namespace std;
using namespace paxos;
using namespace test;

void CheckChosen(
        std::map<uint64_t, 
            std::unique_ptr<test::StorageHelper>>& map_storage, 
        std::map<uint64_t, std::unique_ptr<paxos::Paxos>>& map_paxos, 
        uint64_t prop_index, 
        const std::string& prop_value)
{
    for (auto& id_paxos : map_paxos) {
        auto& paxos = id_paxos.second;
        assert(nullptr != paxos);

        assert(prop_index == paxos->GetMaxIndex());
        assert(prop_index == paxos->GetCommitedIndex());
        paxos->Wait(prop_index);

        auto& storage = map_storage[id_paxos.first];
        assert(nullptr != storage);

        auto hs = storage->read(paxos->GetLogId(), prop_index);
        assert(nullptr != hs);
        assert(prop_index == hs->index());
        assert(0ull < hs->proposed_num());
        assert(0ull < hs->promised_num());
        assert(0ull < hs->accepted_num());
        assert(prop_value == hs->accepted_value().data());

        assert(paxos->GetLogId() == hs->logid());
        assert(0 < hs->seq());
    }
}

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
        prop_value = prop_msg->accepted_value().data();
    }

    auto err = paxos::ErrorCode::OK;
    auto prop_index = 0ull;
    auto eid = 0ull;
    tie(err, prop_index, eid) = paxos->Propose(0ull, prop_value);
    assert(paxos::ErrorCode::OK == err);
    assert(0ull < prop_index);
    assert(0ull < eid);

    // 0. retry other propose failed
    {
        auto new_prop_index = 0ull;
        auto new_eid = 0ull;
        tie(err, new_prop_index, new_eid) = paxos->Propose(0ull, "");
        assert(paxos::ErrorCode::BUSY == err);
    }

    // 1. msg prop
    assert(false == sender.empty());
    auto apply_count = sender.apply(map_paxos);
    assert(size_t{2} == apply_count);

    // 2. msg prop rsp
    assert(false == sender.empty());
    apply_count = sender.apply(map_paxos);
    assert(size_t{2} == apply_count);

    // 3. msg accpt
    assert(false == sender.empty());
    apply_count = sender.apply(map_paxos);
    assert(size_t{2} == apply_count);

    // 4. msg accpt rsp
    assert(false == sender.empty());
    apply_count = sender.apply(map_paxos);
    assert(size_t{2} == apply_count);

    // 5. msg chosen
    assert(false == sender.empty());
    apply_count = sender.apply(map_paxos);
    assert(size_t{2} < apply_count);

    // => all mark as chosen now
    CheckChosen(map_storage, map_paxos, prop_index, prop_value);
}


TEST(PaxosTest, FastProp)
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

    auto err = paxos::ErrorCode::OK;
    auto prop_index = 1ull;
    auto eid = 0ull;
    // 1. prop => chosen
    {
        tie(err, prop_index, eid) = paxos->Propose(0ull, genPropValue());
        assert(paxos::ErrorCode::OK == err);
        assert(0ull < prop_index);
        assert(0ull < eid);

        auto count = sender.apply_until(map_paxos);
        assert(5 == count);
        assert(true == sender.empty());
        assert(prop_index == paxos->GetCommitedIndex());
        assert(prop_index == paxos->GetMaxIndex());
    }

    // 2. next prop => fast prop
    for (int i = 0; i < 10; ++i) {
        assert(true == sender.empty());
        auto prop_value = genPropValue();
        eid = 0ull;
        tie(err, prop_index, eid) = paxos->Propose(0ull, prop_value);
        assert(paxos::ErrorCode::OK == err);
        assert(0ull < prop_index);
        assert(0ull< eid);
        assert(paxos->GetCommitedIndex() + 1ull == prop_index);

        auto count = sender.apply_until(map_paxos);
        assert(3 == count);
        assert(true == sender.empty());
        assert(prop_index == paxos->GetCommitedIndex());
        assert(prop_index == paxos->GetMaxIndex());

        CheckChosen(map_storage, map_paxos, prop_index, prop_value);
    }
}


TEST(PaxosTest, RandomIterPropose)
{
    auto logid = LOGID;
    auto group_ids = GROUP_IDS;

    SendHelper sender{0};
    map<uint64_t, unique_ptr<StorageHelper>> map_storage;
    map<uint64_t, unique_ptr<Paxos>> map_paxos;
    
    tie(map_storage, map_paxos) = 
        build_paxos(logid, group_ids, sender, 0);

    for (auto i = 0; i < 10; ++i) {
        auto prop_id = i % (group_ids.size()) + 1ull;
        assert(0ull < prop_id);
        auto& paxos = map_paxos[prop_id];
        assert(nullptr != paxos);

        assert(true == sender.empty());
        auto err = paxos::ErrorCode::OK;
        auto prop_index = 0ull;
        auto prop_value = genPropValue();
        auto eid = 0ull;
        tie(err, prop_index, eid) = paxos->Propose(0ull, prop_value);
        assert(paxos::ErrorCode::OK == err);
        assert(0ull < prop_index);
        assert(0ull < eid);
        assert(paxos->GetCommitedIndex() < prop_index);

        auto count = sender.apply_until(map_paxos);
        assert(0 < count);
        assert(true == sender.empty());
        assert(prop_index == paxos->GetCommitedIndex());
        assert(prop_index == paxos->GetMaxIndex());

        CheckChosen(map_storage, map_paxos, prop_index, prop_value);
    }
}


TEST(PaxosTest, PropTestWithMsgDrop)
{
    auto logid = LOGID;
    auto group_ids = GROUP_IDS;

    SendHelper sender{50};
    map<uint64_t, unique_ptr<StorageHelper>> map_storage;
    map<uint64_t, unique_ptr<Paxos>> map_paxos;
    tie(map_storage, map_paxos) = 
        build_paxos(logid, group_ids, sender, 60);

    auto selfid = 1ull;
    auto& paxos = map_paxos[selfid];
    assert(nullptr != paxos);
    for (int i = 0; i < 20; ++i) {
        auto prop_value = genPropValue();
        auto err = paxos::ErrorCode::OK;
        auto prop_index = 0ull;
        auto eid = 0ull;
            
        tie(err, prop_index, eid) = paxos->Propose(0ull, prop_value);
        while (true) {
            if (0ull == prop_index) {
                // possible write error:
                err = paxos->CheckAndFixTimeout(chrono::milliseconds{0});
                if (paxos::ErrorCode::OK != err) {
                    assert(paxos::ErrorCode::NO_TIMEOUT_INDEX != err);
                    continue;
                }

                assert(err == paxos::ErrorCode::OK);
                prop_index = paxos->GetMaxIndex();
                assert(paxos->GetCommitedIndex() + 1ull == prop_index);
            }

            assert(0ull < prop_index);
            sender.apply_until(map_paxos);
            if (paxos->IsChosen(prop_index)) {
                break;
            }

            auto peer_id = 2ull;
            assert(peer_id != selfid);
            auto& peer_paxos = map_paxos[peer_id];
            assert(nullptr != peer_paxos);
            
            auto peer_prop_index = 0ull;
            auto peer_eid = 0ull;
            tie(err, peer_prop_index, peer_eid) = peer_paxos->Propose(0ull, "");
            if (paxos::ErrorCode::OK != err || 
                    peer_prop_index > prop_index) {
                err = paxos->CheckAndFixTimeout(chrono::milliseconds{0});
                continue;
            }
        }
    }
}

