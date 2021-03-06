#include "gtest/gtest.h"
#include "paxos.pb.h"
#include "test_helper.h"
#include "paxos.h"
#include "mem_utils.h"
#include "id_utils.h"
#include "hassert.h"


using namespace std;
using namespace paxos;
using namespace test;
using cutils::prop_num_compose;

const uint32_t test_timeout = 10;


void AssertCheckConfState(
        const paxos::ConfState& conf_state, 
        const paxos::ConfState& expected_conf_state)
{
    assert(conf_state.nodes_size() == expected_conf_state.nodes_size());

    set<uint64_t> group_ids;
    set<uint64_t> expected_group_ids;
    for (int i = 0; i < conf_state.nodes_size(); ++i) {
        group_ids.insert(conf_state.nodes(i));
        expected_group_ids.insert(expected_conf_state.nodes(i));
    }

    assert(group_ids == expected_group_ids);
}

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

        int err = 0;
        std::unique_ptr<HardState> hs = nullptr;
        tie(err, hs) = storage->read(paxos->GetLogId(), prop_index);
        assert(0 == err);
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
        build_paxos(logid, test_timeout, group_ids, sender, 0);
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
        build_paxos(logid, test_timeout, group_ids, sender, 0);
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
    auto reqid = 0ull;
    tie(err, prop_index) = paxos->Propose(0ull, reqid, prop_value);
    assert(paxos::ErrorCode::OK == err);
    assert(0ull < prop_index);

    // 0. retry other propose failed
    {
        auto new_prop_index = 0ull;
        auto new_reqid = 0ull;
        tie(err, new_prop_index) = paxos->Propose(0ull, new_reqid, "");
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
        build_paxos(logid, test_timeout, group_ids, sender, 0);
    assert(map_paxos.size() == group_ids.size());
    assert(map_storage.size() == group_ids.size());

    auto& paxos = map_paxos[selfid];
    assert(nullptr != paxos);

    auto err = paxos::ErrorCode::OK;
    auto prop_index = 1ull;
    auto reqid = 1ull;
    // 1. prop => chosen
    {
        tie(err, prop_index) = paxos->Propose(0ull, reqid, genPropValue());
        assert(paxos::ErrorCode::OK == err);
        assert(0ull < prop_index);

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
        tie(err, prop_index) = paxos->Propose(0ull, reqid, prop_value);
        assert(paxos::ErrorCode::OK == err);
        assert(0ull < prop_index);
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
        build_paxos(logid, test_timeout, group_ids, sender, 0);

    auto reqid = 2ull;
    for (auto i = 0; i < 10; ++i) {
        auto prop_id = i % (group_ids.size()) + 1ull;
        assert(0ull < prop_id);
        auto& paxos = map_paxos[prop_id];
        assert(nullptr != paxos);

        assert(true == sender.empty());
        auto err = paxos::ErrorCode::OK;
        auto prop_index = 0ull;
        auto prop_value = genPropValue();
        tie(err, prop_index) = paxos->Propose(0ull, reqid, prop_value);
        assert(paxos::ErrorCode::OK == err);
        assert(0ull < prop_index);
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
        build_paxos(logid, test_timeout, group_ids, sender, 60);

    auto selfid = 1ull;
    auto& paxos = map_paxos[selfid];
    assert(nullptr != paxos);
    for (int i = 0; i < 30; ++i) {
        auto prop_value = genPropValue();
        auto err = paxos::ErrorCode::OK;
        auto prop_index = 0ull;
            
        auto reqid = 0ull;
        tie(err, prop_index) = paxos->Propose(0ull, reqid, prop_value);
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
            tie(err, peer_prop_index) = peer_paxos->Propose(0ull, reqid, "");
            if (paxos::ErrorCode::OK != err || 
                    peer_prop_index > prop_index) {
                err = paxos->CheckAndFixTimeout(chrono::milliseconds{0});
                continue;
            }
        }

        for (auto id : group_ids) {
            auto& peer_paxos = map_paxos[id];
            assert(nullptr != peer_paxos);

            if (true == peer_paxos->IsChosen(prop_index)) {
                continue;
            }

            while (false == peer_paxos->IsChosen(prop_index)) {
                auto err = peer_paxos->CheckAndFixTimeout(chrono::milliseconds{0});
                sender.apply_until(map_paxos);
            }
        }
    }
}

TEST(PaxosTest, ProposeCatchUp)
{
    auto logid = LOGID;
    auto group_ids = GROUP_IDS;

    auto selfid = 1ul;
    SendHelper sender{0};
    map<uint64_t, unique_ptr<StorageHelper>> map_storage;
    map<uint64_t, unique_ptr<Paxos>> map_paxos;

    tie(map_storage, map_paxos) = 
        build_paxos(logid, test_timeout, group_ids, sender, 0);
    assert(map_paxos.size() == group_ids.size());
    assert(map_storage.size() == group_ids.size());

    auto& paxos = map_paxos[selfid];
    assert(nullptr != paxos);

    auto peer_id = 2ull;
    auto peer_paxos = move(map_paxos[peer_id]);
    assert(nullptr != peer_paxos);
    assert(nullptr == map_paxos[peer_id]);

    uint64_t reqid = 1ull;
    auto err = paxos::ErrorCode::OK;
    uint64_t prop_index = 0;
    for (int i = 0; i < 15; ++i) {
        assert(true == sender.empty());
        auto prop_value = genPropValue();
        tie(err, prop_index) = paxos->Propose(0ull, reqid, prop_value);
        assert(paxos::ErrorCode::OK == err);
        assert(0ull < prop_index);
        assert(paxos->GetCommitedIndex() + 1ull == prop_index);

        sender.apply_until(map_paxos);
    }

    map_paxos[peer_id] = move(peer_paxos);
    assert(nullptr != map_paxos[peer_id]);
    assert(nullptr == peer_paxos);
    for (int i = 0; i < 20; ++i) {
        assert(true == sender.empty());
        auto prop_value = genPropValue();
        tie(err, prop_index) = paxos->Propose(0ull, reqid, prop_value);
        assert(paxos::ErrorCode::OK == err);
        assert(0ull < prop_index);
        assert(paxos->GetCommitedIndex() + 1ull == prop_index);

        sender.apply_until(map_paxos);
    }

    for (auto peer_id : group_ids) {
        auto& peer_paxos = map_paxos[peer_id];
        assert(peer_paxos->GetMaxIndex() == 
                peer_paxos->GetCommitedIndex());
        assert(paxos->GetCommitedIndex() == 
                peer_paxos->GetCommitedIndex());
    }
}

TEST(PaxosTest, ZeroIndex)
{
    auto logid = LOGID;
    auto group_ids = GROUP_IDS;
    
    auto selfid = 1ull;
    SendHelper sender{0};
    map<uint64_t, unique_ptr<StorageHelper>> map_storage;
    map<uint64_t, unique_ptr<Paxos>> map_paxos;

    tie(map_storage, map_paxos) = 
        build_paxos(logid, test_timeout, group_ids, sender, 0);

    auto& paxos = map_paxos[selfid];
    assert(nullptr != paxos);

    // case 1
    {
        // zero index, nothing timeout
        assert(true == sender.empty());
        paxos::Message msg;
        msg.set_type(paxos::MessageType::NOOP);
        msg.set_logid(logid);
        msg.set_index(0ull);
        msg.set_to(selfid);
        auto err = paxos->Step(msg);
        assert(paxos::ErrorCode::OK == err);
        assert(true == sender.empty());
    }

    // case 2
    uint64_t reqid = 1ull;
    uint64_t prop_index = 0;
    {
        auto err = paxos::ErrorCode::OK;
        auto prop_value = genPropValue();
        tie(err, prop_index) = paxos->Propose(0ull, reqid, prop_value);
        assert(paxos::ErrorCode::OK == err);
        assert(1ull == prop_index);
        sender.drop_all();

        assert(true == sender.empty());
        paxos::Message msg;
        msg.set_type(paxos::MessageType::NOOP);
        msg.set_logid(logid);
        msg.set_index(0ull);
        msg.set_to(selfid);
        err = paxos->Step(msg);
        assert(paxos::ErrorCode::OK == err);
        assert(true == sender.empty()); // nothing timeout
    }

    // case 3
    {
        usleep((test_timeout + 2) * 1000);
        assert(true == sender.empty());
        paxos::Message msg;
        msg.set_type(paxos::MessageType::NOOP);
        msg.set_logid(logid);
        msg.set_index(0ull);
        msg.set_to(selfid);
        auto err = paxos->Step(msg);
        assert(paxos::ErrorCode::OK == err);
        assert(false == sender.empty()); // should be timeout

        sender.apply_until(map_paxos);
        assert(prop_index == paxos->GetCommitedIndex());

        auto ret = paxos->CheckChosen(prop_index, reqid);
        assert(1 == ret);

        ret = paxos->CheckChosen(prop_index, 0);
        assert(0 == ret);
    }
}

TEST(PaxosTest, EmptySnapshotMeta)
{
    SnapshotMetadata meta; 
    meta.set_logid(LOGID);
    {
        auto conf_state = meta.mutable_conf_state();
        assert(nullptr != conf_state);
        for (auto id : GROUP_IDS) {
            conf_state->add_nodes(id);
        }
        assert(true == meta.has_conf_state());
    }

    meta.set_commited_index(0);
    
    auto selfid = 1ull;
    PaxosCallBack callback;
    callback.read = 
        [](uint64_t logid, uint64_t idnex) 
            -> std::tuple<int, std::unique_ptr<HardState>> {

            return make_tuple(1, nullptr);
        };

    callback.write = 
        [](const std::vector<std::unique_ptr<HardState>>& vec_hs) -> int {
            return 0;
        };

    callback.send = 
        [](std::vector<std::unique_ptr<Message>> vec_msg) -> int {
            return 0;
        };

    auto paxos = cutils::make_unique<Paxos>(
            selfid, test_timeout, meta, callback);
    assert(nullptr != paxos);
    assert(0ull == paxos->GetMaxIndex());
    assert(0ull == paxos->GetCommitedIndex());
    assert(selfid == paxos->GetSelfId());
    assert(LOGID == paxos->GetLogId());

    auto new_meta = paxos->CreateSnapshotMetadata();
    assert(nullptr != new_meta);

    assert(true == new_meta->has_conf_state());
    AssertCheckConfState(new_meta->conf_state(), meta.conf_state());
    assert(LOGID == new_meta->logid());
    assert(0 == new_meta->commited_index());
}

TEST(PaxosTest, SnapshotMetadataConstruct)
{
    ConfState conf_state;
    for (auto id : GROUP_IDS) {
        conf_state.add_nodes(id);
    }

    PaxosCallBack callback;
    callback.write = 
        [](const std::vector<std::unique_ptr<HardState>>& vec_hs) -> int {
            return 0;
        };

    callback.send = 
        [](std::vector<std::unique_ptr<Message>> vec_msg) -> int {
            return 0;
        };

    auto test_index = 10ull;
    auto selfid = 1ull;
    auto simple_read_cb = 
        [=](uint64_t logid, uint64_t index) 
            -> std::tuple<int, std::unique_ptr<HardState>> {

            if (index > test_index) {
                return make_tuple(1, nullptr);
            }

            auto hs = cutils::make_unique<HardState>();
            assert(nullptr != hs);
            hs->set_index(index);
            hs->set_logid(logid);
            hs->set_proposed_num(
                    prop_num_compose(static_cast<uint8_t>(selfid), 0ull));
            hs->set_seq(1ull);

            return make_tuple(0, move(hs));
        };

    // case 1
    {
        SnapshotMetadata meta;
        meta.set_logid(LOGID);
        assert(nullptr != meta.mutable_conf_state());
        *(meta.mutable_conf_state()) = conf_state;

        meta.set_commited_index(test_index);
        callback.read = simple_read_cb;

        auto paxos = cutils::make_unique<Paxos>(
                selfid, test_timeout, meta, callback);
        assert(nullptr != paxos);
        assert(test_index == paxos->GetMaxIndex());
        assert(test_index == paxos->GetCommitedIndex());
        assert(true == paxos->IsChosen(test_index));

        auto new_meta = paxos->CreateSnapshotMetadata();
        assert(nullptr != new_meta);

        assert(true == new_meta->has_conf_state());
        AssertCheckConfState(new_meta->conf_state(), conf_state);
        assert(LOGID == new_meta->logid());
        assert(test_index == new_meta->commited_index());
    }

    callback.read = nullptr;
    // case 2
    {
        SnapshotMetadata meta;
        meta.set_logid(LOGID);
        assert(nullptr != meta.mutable_conf_state());
        *(meta.mutable_conf_state()) = conf_state;

        meta.set_commited_index(0);
        callback.read = simple_read_cb;

        auto paxos = cutils::make_unique<Paxos>(
                selfid, test_timeout, meta, callback);
        assert(nullptr != paxos);
        assert(test_index == paxos->GetMaxIndex());
        assert(test_index - 1 == paxos->GetCommitedIndex());
        assert(false == paxos->IsChosen(test_index));
        assert(true == paxos->IsChosen(test_index - 1));

        auto new_meta = paxos->CreateSnapshotMetadata();
        assert(nullptr != new_meta);

        assert(true == new_meta->has_conf_state());
        AssertCheckConfState(new_meta->conf_state(), conf_state);
        assert(LOGID == new_meta->logid());
        assert(test_index - 1 == new_meta->commited_index());
    }
}

