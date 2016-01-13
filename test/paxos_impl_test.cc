#include "gtest/gtest.h"
#include "paxos.pb.h"
#include "test_helper.h"
#include "paxos_instance.h"
#include "paxos_impl.h"


using namespace std;
using namespace paxos;
using namespace test;

TEST(PaxosImplTest, SimpleConstruct)
{
    auto logid = LOGID;
    auto group_ids = GROUP_IDS;

    auto selfid = 1ull;
    assert(group_ids.end() != group_ids.find(selfid));
    auto paxos = make_unique<PaxosImpl>(logid, selfid, group_ids);
    assert(nullptr != paxos);
    assert(paxos->GetSelfId() == selfid);
    assert(paxos->GetLogId() == logid);
    assert(0ull == paxos->GetCommitedIndex());
    assert(0ull == paxos->GetMaxIndex());
}

TEST(PaxosImplTest, SimplePropose)
{
    auto logid = LOGID;
    auto group_ids = GROUP_IDS;

    auto map_paxos = build_paxos(logid, group_ids);
    assert(map_paxos.size() == group_ids.size());

    auto selfid = 1ull;
    auto& paxos = map_paxos[selfid];
    assert(nullptr != paxos);
    
    std::string prop_value;
    vector<unique_ptr<Message>> vec_msg;
    // 1. prop
    {
        auto prop_index = paxos->NextProposingIndex();
        assert(1ull == prop_index);
        auto prop_msg = buildMsgProp(logid, selfid, prop_index);
        assert(nullptr != prop_msg);
        prop_value = prop_msg->accepted_value();
        assert(false == prop_value.empty());
        assert(selfid == prop_msg->to());

        auto rsp_msg_type = Step(*paxos, *prop_msg, nullptr);
        assert(MessageType::PROP == rsp_msg_type);
        {
            assert(prop_msg->index() == paxos->GetMaxIndex());
            auto ins = paxos->GetInstance(prop_msg->index(), false);
            assert(nullptr != ins);
            assert(true == ins->GetStrictPropFlag());
            assert(PropState::WAIT_PREPARE == ins->GetPropState());
        }
        
        vec_msg = ProduceRsp(*paxos, *prop_msg, rsp_msg_type, nullptr);
        assert(vec_msg.size() == group_ids.size() - 1);
        for (auto& rsp_msg : vec_msg) {
            assert(nullptr != rsp_msg);
            assert(MessageType::PROP == rsp_msg->type());
            assert(selfid == rsp_msg->from());
            assert(logid == rsp_msg->logid());
            assert(prop_msg->index() == rsp_msg->index());
            assert(0ull < rsp_msg->proposed_num());
        }
    }

    // 2. send prop to peers
    {
        vec_msg = apply(map_paxos, vec_msg);
        assert(vec_msg.size() == group_ids.size() - 1);
        for (auto& rsp_msg : vec_msg) {
            assert(nullptr != rsp_msg);
            assert(MessageType::PROP_RSP == rsp_msg->type());
            assert(selfid == rsp_msg->to());
            assert(logid == rsp_msg->logid());
            assert(0ull < rsp_msg->proposed_num());
            assert(rsp_msg->proposed_num() == rsp_msg->promised_num());
            assert(0ull == rsp_msg->accepted_num());
            assert(0ull < rsp_msg->index());
            {
                auto& peer_paxos = map_paxos[rsp_msg->from()];
                assert(nullptr != peer_paxos);
                auto ins = peer_paxos->GetInstance(
                        rsp_msg->index(), false);
                assert(nullptr != ins);
                assert(false == ins->GetStrictPropFlag());
                assert(ins->GetPromisedNum() == rsp_msg->promised_num());
            }
        }
    }

    // 3. peers send prop rsp back to selfid
    {
        vec_msg = apply(map_paxos, vec_msg);
        assert(vec_msg.size() == group_ids.size() - 1);
        {
            auto index = paxos->GetMaxIndex();
            // check selfid stat
            auto ins = paxos->GetInstance(index, false);
            assert(nullptr != ins);
            assert(true == ins->GetStrictPropFlag());
            assert(PropState::WAIT_ACCEPT == ins->GetPropState());
        }

        for (auto& rsp_msg : vec_msg) {
            assert(nullptr != rsp_msg);
            assert(MessageType::ACCPT == rsp_msg->type());
            assert(selfid == rsp_msg->from());
            assert(logid == rsp_msg->logid());
            assert(0ull < rsp_msg->proposed_num());
        }
    }

    // 4. selfid send accpt to peers
    {
        vec_msg = apply(map_paxos, vec_msg);
        assert(vec_msg.size() == group_ids.size() - 1);
        for (auto& rsp_msg : vec_msg) {
            assert(nullptr != rsp_msg);
            assert(MessageType::ACCPT_RSP == rsp_msg->type());
            assert(0ull < rsp_msg->proposed_num());
            assert(rsp_msg->proposed_num() == rsp_msg->accepted_num());
            {
                auto& peer_paxos = map_paxos[rsp_msg->from()];
                assert(nullptr != peer_paxos);
                auto ins = 
                    peer_paxos->GetInstance(rsp_msg->index(), false);
                assert(nullptr != ins);
                assert(false == ins->GetStrictPropFlag());
                assert(ins->GetAcceptedNum() == rsp_msg->accepted_num());
                assert(prop_value == ins->GetAcceptedValue());
            }
        }
    }

    // 5. peers send accpt_rsp to selfid
    {
        vec_msg = apply(map_paxos, vec_msg);
        assert(false == vec_msg.empty());
        {
            assert(paxos->GetCommitedIndex() == paxos->GetMaxIndex());
            auto index = paxos->GetMaxIndex();
            auto ins = paxos->GetInstance(index, false);
            assert(nullptr != ins);
            assert(true == ins->GetStrictPropFlag());
            assert(PropState::CHOSEN == ins->GetPropState());
            assert(true == paxos->IsChosen(index));
        }
        for (auto& rsp_msg : vec_msg) {
            assert(nullptr != rsp_msg);
            assert(MessageType::CHOSEN == rsp_msg->type());
            assert(0ull < rsp_msg->accepted_num());
        }
    }

    // 7. selfid send chosen to peers
    {
        vec_msg = apply(map_paxos, vec_msg);
        assert(true == vec_msg.empty());
        auto index = paxos->GetCommitedIndex();
        assert(paxos->GetMaxIndex() == index);
        for (auto id : group_ids) {
            auto& peer_paxos = map_paxos[id];
            assert(nullptr != peer_paxos);
            assert(peer_paxos->GetMaxIndex() == 
                    peer_paxos->GetCommitedIndex());
            assert(index == peer_paxos->GetCommitedIndex());
            auto ins = peer_paxos->GetInstance(index, false);
            assert(nullptr != ins);
            assert((selfid == id) == ins->GetStrictPropFlag());
            assert(ins->GetAcceptedValue() == prop_value);
            assert(PropState::CHOSEN == ins->GetPropState());
            assert(true == peer_paxos->IsChosen(index));
        }
    }
}

TEST(PaxosImplTest, FastProp)
{
    auto logid = LOGID;
    auto group_ids = GROUP_IDS;

    auto map_paxos = build_paxos(logid, group_ids);
    assert(map_paxos.size() == group_ids.size());

    auto selfid = 1ull;
    auto& paxos = map_paxos[selfid];
    assert(nullptr != paxos);

    vector<unique_ptr<Message>> vec_msg;
    // 1. first prop
    {
        auto prop_index = paxos->NextProposingIndex();
        assert(1ull == prop_index);
        auto prop_msg = buildMsgProp(logid, selfid, prop_index);
        assert(nullptr != prop_msg);
        assert(false == paxos->CanFastProp(prop_msg->index()));

        vec_msg.emplace_back(move(prop_msg));
        apply_until(map_paxos, move(vec_msg));
        for (const auto& id_paxos : map_paxos) {
            const auto& peer_paxos = id_paxos.second;
            assert(nullptr != peer_paxos);
            assert(peer_paxos->GetCommitedIndex() == 
                    peer_paxos->GetMaxIndex());
            assert(peer_paxos->GetCommitedIndex() == prop_index);
            assert(true == peer_paxos->IsChosen(prop_index));
        }
    }

    // 2. fast prop : repeat test
    for (int i = 0; i < 10; ++i) {
        assert(true == vec_msg.empty());
        // 2.1  selfid fast accept to peers
        auto prop_index = paxos->NextProposingIndex();
        assert(1ull < prop_index);
        assert(true == paxos->CanFastProp(prop_index));
        {
            auto prop_msg = buildMsgProp(logid, selfid, prop_index); 
            assert(nullptr != prop_msg);
            prop_msg->set_type(MessageType::BEGIN_FAST_PROP);

            vec_msg.emplace_back(move(prop_msg));
            vec_msg = apply(map_paxos, vec_msg);
            assert(vec_msg.size() == group_ids.size() - 1);
            for (auto& rsp_msg : vec_msg) {
                assert(nullptr != rsp_msg);
                assert(selfid == rsp_msg->from());
                assert(MessageType::FAST_ACCPT == rsp_msg->type());
                assert(prop_index == rsp_msg->index());
                assert(0ull < rsp_msg->proposed_num());
            }

            {
                auto ins = paxos->GetInstance(prop_index, false);
                assert(nullptr != ins);
                assert(true == ins->GetStrictPropFlag());
            }
        }

        // 2.2 peers recv fast accept, send back fast accpt rsp
        {
            vec_msg = apply(map_paxos, vec_msg);
            assert(vec_msg.size() == group_ids.size() - 1);
            for (auto& rsp_msg : vec_msg) {
                assert(nullptr != rsp_msg);
                assert(selfid == rsp_msg->to());
                assert(MessageType::FAST_ACCPT_RSP == rsp_msg->type());
                assert(0ull < rsp_msg->proposed_num());
                {
                    auto& peer_paxos = map_paxos[rsp_msg->from()];
                    assert(nullptr != peer_paxos);
                    auto ins = peer_paxos->GetInstance(
                            rsp_msg->index(), false);
                    assert(nullptr != ins);
                    assert(false == ins->GetStrictPropFlag());
                }
            }
        }

        // 2.3 self recv fast accpt rsp, mark ins as chosen(broad-cast)
        {
            vec_msg = apply(map_paxos, vec_msg);
            assert(false == vec_msg.empty());
            {
                auto ins = paxos->GetInstance(prop_index, false);
                assert(nullptr != ins);
                assert(true == ins->GetStrictPropFlag());
                assert(PropState::CHOSEN == ins->GetPropState());
                assert(true == paxos->IsChosen(prop_index));
                assert(paxos->GetCommitedIndex() == prop_index);
                assert(paxos->GetCommitedIndex() == paxos->GetMaxIndex());
            }
            for (auto& rsp_msg : vec_msg) {
                assert(nullptr != rsp_msg);
                assert(MessageType::CHOSEN == rsp_msg->type());
                assert(0ull < rsp_msg->accepted_num());
            }
        }

        // 2.4 peers recv chosen
        {
            vec_msg = apply(map_paxos, vec_msg);
            assert(true == vec_msg.empty());
            for (auto id : group_ids) {
                auto& peer_paxos = map_paxos[id];
                assert(nullptr != peer_paxos);
                assert(peer_paxos->GetMaxIndex() == 
                            peer_paxos->GetCommitedIndex());
                assert(prop_index == 
                        peer_paxos->GetCommitedIndex());
                
                auto ins = peer_paxos->GetInstance(prop_index, false);
                assert(nullptr != ins);
                assert((selfid == id) == ins->GetStrictPropFlag());
                assert(PropState::CHOSEN == ins->GetPropState());
                assert(true == peer_paxos->IsChosen(prop_index));
            }
        }
    }

    // 3. end fast accpt 
    auto peer_id = 2ull;
    assert(peer_id != selfid);
    auto& peer_paxos = map_paxos[peer_id];
    assert(nullptr != peer_paxos);
    assert(true == vec_msg.empty());
    {
        auto prop_index = peer_paxos->NextProposingIndex();
        assert(0ull < prop_index);
        assert(true == paxos->CanFastProp(prop_index));
        assert(false == peer_paxos->CanFastProp(prop_index));
        auto prop_msg = buildMsgProp(logid, peer_id, prop_index);
        assert(nullptr != prop_msg);

        vec_msg.emplace_back(move(prop_msg));
        // 1. 
        {
            vec_msg = apply(map_paxos, vec_msg);
            assert(vec_msg.size() == group_ids.size() - 1);
            {
                auto ins = peer_paxos->GetInstance(prop_index, false);
                assert(nullptr != ins);
                assert(true == ins->GetStrictPropFlag());
            }
        }
        apply_until(map_paxos, move(vec_msg));
        for (const auto& id_paxos : map_paxos) {
            auto& other_paxos = id_paxos.second;
            assert(nullptr != other_paxos);
            assert(prop_index == other_paxos->GetCommitedIndex());
            assert(true == other_paxos->IsChosen(prop_index));
            logdebug("id %" PRIu64 " prop_index %" PRIu64 " can fast prop %d", 
                    id_paxos.first, prop_index, other_paxos->CanFastProp(prop_index));
            assert((peer_id == id_paxos.first) == 
                    other_paxos->CanFastProp(prop_index + 1ull));
        }

        assert(true == peer_paxos->CanFastProp(prop_index + 1ull));
        assert(false == paxos->CanFastProp(prop_index + 1ull));
    }
}

TEST(PaxosImplTest, RandomIterPropose)
{
    auto logid = LOGID;
    auto group_ids = GROUP_IDS;

    auto map_paxos = build_paxos(logid, group_ids);
    assert(map_paxos.size() == group_ids.size());

    for (auto i = 0; i < 10; ++i) {
        auto prop_id = i % (group_ids.size()) + 1ull;
        assert(0 < prop_id);
        auto& paxos = map_paxos[prop_id];
        assert(nullptr != paxos);

        auto prop_index = paxos->NextProposingIndex();
        assert(0ull < prop_index);
        assert(false == paxos->CanFastProp(prop_index));

        for (auto count = 0; count < 3; ++count) {
            prop_index = paxos->NextProposingIndex();
            assert(0ull < prop_index);
            vector<unique_ptr<Message>> vec_msg;
            vec_msg.emplace_back(buildMsgProp(logid, prop_id, prop_index));
            apply_until(map_paxos, move(vec_msg));
        }

        prop_index = paxos->NextProposingIndex();
        for (auto id : group_ids) {
            auto& peer_paxos = map_paxos[id];
            assert(nullptr != peer_paxos);

            assert(peer_paxos->GetCommitedIndex() == 
                    peer_paxos->GetMaxIndex());
            assert((prop_id == id) == peer_paxos->CanFastProp(prop_index));
        }
    }
}

