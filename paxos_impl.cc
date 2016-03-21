#include <sstream>
#include "paxos_impl.h"
#include "paxos_instance.h"
#include "mem_utils.h"
#include "log_utils.h"
#include "id_utils.h"
#include "hassert.h"


using namespace std;

namespace paxos {

const size_t MAX_INS_SIZE = 5; // TODO: config option ?

} // namespace paxos

namespace {

using namespace paxos;



std::unique_ptr<PaxosInstance> buildPaxosInstance(
        size_t group_size, uint64_t selfid, uint64_t prop_cnt)
{
    const int major_cnt = static_cast<int>(group_size)/2 + 1;
    assert(0 < major_cnt);
    assert(0 < selfid);
    
    uint64_t prop_num = cutils::prop_num_compose(selfid, prop_cnt); 
    assert(0 < prop_num);
    
    auto new_ins = cutils::make_unique<PaxosInstance>(major_cnt, prop_num);
    assert(nullptr != new_ins);
    return new_ins;
}

std::vector<std::unique_ptr<paxos::Message>>
batchBuildMsg(
        const uint64_t exclude_id, 
        const std::set<uint64_t>& group_ids, 
        const paxos::Message& msg_template)
{
    vector<unique_ptr<Message>> vec_msg;
    vec_msg.reserve(group_ids.size() - 1);
    for (auto id : group_ids) {
        if (exclude_id == id) {
            continue;
        }

        auto msg = cutils::make_unique<Message>(msg_template);
        assert(nullptr != msg);
        msg->set_to(id);
        vec_msg.emplace_back(move(msg));
    }
    return vec_msg;
}

std::vector<std::unique_ptr<paxos::Message>>
batchBuildMsg(
        const uint64_t exclude_id, 
        const std::set<uint64_t>& group_ids, 
        std::unique_ptr<Message> rsp_msg)
{
    assert(nullptr != rsp_msg);
    assert(0ull == rsp_msg->to());

    vector<unique_ptr<Message>> vec_msg;
    vec_msg.reserve(group_ids.size() - 1);
    for (auto id : group_ids) {
        if (exclude_id == id) {
            continue;
        }

        auto msg = cutils::make_unique<Message>(*rsp_msg);
        assert(nullptr != msg);
        msg->set_to(id);
        vec_msg.emplace_back(move(msg));
    }

    return vec_msg;
}

PaxosInstance* GetInstance(
        PaxosImpl& paxos_impl, uint64_t index, PaxosInstance* disk_ins)
{
    if (nullptr != disk_ins) {
        assert(PropState::CHOSEN == disk_ins->GetPropState());
        assert(nullptr == paxos_impl.GetInstance(index, false));
        assert(index < paxos_impl.GetCommitedIndex());
        return disk_ins;
    }

    assert(nullptr == disk_ins);
    return paxos_impl.GetInstance(index, true);
}

} // namespace


namespace paxos {

PaxosImpl::PaxosImpl(
        uint64_t logid, 
        uint64_t selfid, 
        const std::set<uint64_t>& group_ids)
    : logid_(logid)
    , selfid_(selfid)
    , group_ids_(group_ids)
{
    assert(0ull < selfid_);
    assert(selfid_ < (1ull << 8));
    assert(group_ids_.end() != group_ids_.find(selfid_));
}

PaxosImpl::PaxosImpl(
        uint64_t logid, 
        uint64_t selfid, 
        const std::set<uint64_t>& group_ids, 
        const std::deque<std::unique_ptr<paxos::HardState>>& hs_deque, 
        bool all_index_commited)
    : logid_(logid)
    , selfid_(selfid)
    , group_ids_(group_ids)
{
    assert(0ull < selfid_);
    assert(selfid_ < (1ull << 8));
    assert(group_ids_.end() != group_ids_.find(selfid_));

    max_index_ = hs_deque.empty() ? 0ull : hs_deque.back()->index();
    commited_index_ = all_index_commited ? max_index_ : max_index_ -1;
    assert(commited_index_ <= max_index_);
    next_commited_index_ = commited_index_;
    for (auto& hs : hs_deque) {
        assert(nullptr != hs);
        auto prop_state = PropState::CHOSEN;
        if (hs->index() == max_index_ && !all_index_commited) {
            prop_state = PropState::NIL;
        }

        auto ins = cutils::make_unique<PaxosInstance>(
                group_ids_.size() / 2 + 1, prop_state, *hs);
        assert(nullptr != ins);

        assert(ins_map_.end() == ins_map_.find(hs->index()));
        ins_map_[hs->index()] = move(ins);
        assert(nullptr == ins);
    }
}


PaxosImpl::~PaxosImpl() = default;

uint64_t PaxosImpl::NextProposingIndex()
{
    if (max_index_ != commited_index_) {
        return 0;
    }
    
    return max_index_ + 1;
}

std::unique_ptr<PaxosInstance>
PaxosImpl::BuildPaxosInstance(const HardState& hs, PropState prop_state)
{
    const int major_cnt = static_cast<int>(group_ids_.size()) / 2 + 1;
    auto new_ins = cutils::make_unique<PaxosInstance>(major_cnt, prop_state, hs);
    assert(nullptr != new_ins);
    return new_ins;
}

PaxosInstance* PaxosImpl::GetInstance(uint64_t index, bool create)
{
    assert(0 < index);
    if (ins_map_.end() == ins_map_.find(index)) {
        if (false == create) {
            return nullptr;
        }

        assert(true == create);
        if (index <= commited_index_) {
            return nullptr; // don't re-create
        }

        // need build a new paxos instance
        auto new_ins = 
            buildPaxosInstance(group_ids_.size(), selfid_, 0ull);
        assert(nullptr != new_ins);
        ins_map_[index] = move(new_ins);
        assert(nullptr == new_ins);
        max_index_ = max(max_index_, index);
    }

    PaxosInstance* ins = ins_map_[index].get();
    assert(nullptr != ins);
    return ins; 
}

void PaxosImpl::CommitStep(uint64_t index, uint32_t store_seq)
{
    assert(0 < index);
    assert(commited_index_ <= next_commited_index_);

    if (0 != store_seq) {
        auto ins = GetInstance(index, false);
        if (nullptr != ins) {
            ins->CommitPendingSeq(store_seq);
        }
    }

    if (ins_map_.size() >= MAX_INS_SIZE && index < commited_index_) {
        auto ins = GetInstance(index, false);
        if (nullptr != ins) {
            assert(0 == ins->GetPendingSeq());
            ins_map_.erase(index);
            logdebug("GC paxos instance index %" PRIu64, index);
        }

    }

    logdebug("INFO index %" PRIu64 " store_seq %d commited_index %" PRIu64 
            " next_commited_index_ %" PRIu64, 
            index, store_seq, commited_index_, next_commited_index_);
    commited_index_ = next_commited_index_;
}

bool PaxosImpl::UpdateNextCommitedIndex(uint64_t chosen_index) 
{
    // chosen_set_.insert(chosen_index);
    logdebug("selfid %" PRIu64 " mark index %" PRIu64 " as chosen", 
            GetSelfId(), chosen_index);
    if (chosen_index <= next_commited_index_) {
        return false; // update nothing
    }

    uint64_t prev_next_commited_index = next_commited_index_;
    for (auto next = next_commited_index_ + 1; 
            next <= max_index_; ++next) {
        assert(next > commited_index_);
        auto ins = GetInstance(next, false);

        if ((nullptr != ins) && 
                (!ins->IsChosen() || 0ull != ins->GetPendingSeq())) {
            break;
        }

        next_commited_index_ = next;
    }

    assert(prev_next_commited_index <= next_commited_index_);
    logdebug("INFO prev_next_commited_index %" PRIu64 
            " next_commited_index_ %" PRIu64, 
            prev_next_commited_index, next_commited_index_);
    return prev_next_commited_index < next_commited_index_;
}

bool PaxosImpl::CanFastProp(uint64_t prop_index)
{
    assert(0ull < prop_index);
    // 1. check is a prop index
    if (commited_index_ != max_index_ || 
            prop_index != commited_index_ + 1ull || 
            1ull == prop_index) {
        return false;
    }

    // normal propose => possible to do fast prop ?
    // check previous paxos instance => is StrictPropFlag on ?
    assert(commited_index_ == max_index_);
    assert(prop_index == commited_index_ + 1ull);
    assert(1ull < prop_index);
    auto ins = GetInstance(prop_index, false);
    assert(nullptr == ins);
    auto prev_ins = GetInstance(prop_index - 1ull, false);
    assert(nullptr != prev_ins);
    return prev_ins->GetStrictPropFlag();
}

MessageType Step(
        PaxosImpl& paxos_impl, 
        const Message& req_msg, 
        PaxosInstance* disk_ins)
{
    auto ins = GetInstance(paxos_impl, req_msg.index(), disk_ins);
    assert(nullptr != ins);

    return ins->Step(req_msg);
}

std::vector<std::unique_ptr<Message>>
ProduceRsp(
        PaxosImpl& paxos_impl, 
        const Message& req_msg, 
        MessageType rsp_msg_type, 
        PaxosInstance* disk_ins)
{
    assert(req_msg.to() == paxos_impl.GetSelfId());
    assert(req_msg.logid() == paxos_impl.GetLogId());

    auto ins = GetInstance(paxos_impl, req_msg.index(), disk_ins);
    assert(nullptr != ins);

    vector<unique_ptr<Message>> vec_msg;
    auto rsp_msg = ins->ProduceRsp(req_msg, rsp_msg_type);

    logdebug("rsp_msg %p rsp_msg_type %d", 
            rsp_msg.get(), static_cast<int>(rsp_msg_type));
    if (nullptr != rsp_msg) {
        logdebug("selfid %" PRIu64 " rsp_msg_type %d "
                "rsp_msg->to %" PRIu64, 
                paxos_impl.GetSelfId(), 
                static_cast<int>(rsp_msg_type), 
                rsp_msg->to());
        assert(rsp_msg->from() == req_msg.to());
        hassert(MessageType::UNKOWN == rsp_msg_type ||
                rsp_msg->type() == rsp_msg_type, 
                "rsp_msg:type %d rsp_msg_type %d", 
                static_cast<int>(rsp_msg->type()), 
                static_cast<int>(rsp_msg_type));
        assert(rsp_msg->index() == req_msg.index());
        assert(rsp_msg->logid() == req_msg.logid());
        if (0ull == rsp_msg->to()) {
            // broad cast message
            vec_msg = batchBuildMsg(
                    paxos_impl.GetSelfId(), 
                    paxos_impl.GetGroupIds(), move(rsp_msg));
        }
        else {
            vec_msg.emplace_back(move(rsp_msg));
        }
    }
    assert(nullptr == rsp_msg);

    if (MessageType::CHOSEN == rsp_msg_type) {
        // mark req_msg.index as chosen index
        // => msg only send back if pending state have been 
        //    store successfully;
        bool update = paxos_impl.UpdateNextCommitedIndex(req_msg.index());
        logdebug("index %" PRIu64 " commited_index %" PRIu64 
                " next_commited_index_ %" PRIu64 " update %d", 
                req_msg.index(), 
                paxos_impl.GetCommitedIndex(),
                paxos_impl.GetNextCommitedIndex(), 
                static_cast<int>(update));
    }

    return vec_msg;
}

} // namspace paxos


