#include <sstream>
#include "paxos_impl.h"
#include "paxos_instance.h"
#include "utils.h"


using namespace std;

namespace {

using namespace paxos;

const size_t MAX_INS_SIZE = 100; // TODO: config option ?

std::unique_ptr<PaxosInstance> buildPaxosInstance(
        size_t group_size, uint64_t selfid, uint64_t prop_cnt)
{
    const int major_cnt = static_cast<int>(group_size)/2 + 1;
    assert(0 < major_cnt);
    assert(0 < selfid);
    
    uint64_t prop_num = prop_num_compose(selfid, prop_cnt); 
    assert(0 < prop_num);
    auto new_ins = unique_ptr<PaxosInstance>{
        new PaxosInstance{major_cnt, prop_num}};
    assert(nullptr != new_ins);
    return new_ins;
}

} // namespace


namespace paxos {

PaxosImpl::PaxosImpl(uint64_t logid, uint64_t selfid, uint64_t group_size)
    : logid_(logid)
    , selfid_(selfid)
    , group_size_(group_size)
{
    assert(0 < selfid_);
    assert(selfid_ <= group_size_);
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
PaxosImpl::BuildNewPaxosInstance()
{
    return buildPaxosInstance(group_size_, selfid_, 0);
}

std::unique_ptr<PaxosInstance>
PaxosImpl::BuildPaxosInstance(const HardState& hs, PropState prop_state)
{
    const int major_cnt = static_cast<int>(group_size_) / 2 + 1;
    auto new_ins = unique_ptr<PaxosInstance>{
        new PaxosInstance{
            major_cnt, hs.proposed_num(), hs.promised_num(), 
            hs.accepted_num(), hs.accepted_value(), prop_state}};
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
            buildPaxosInstance(group_size_, selfid_, 0);
        assert(nullptr != new_ins);
        ins_map_[index] = move(new_ins);
        assert(nullptr == new_ins);
        max_index_ = max(max_index_, index);
    }

    PaxosInstance* ins = ins_map_[index].get();
    assert(nullptr != ins);
    return ins; 
}

void PaxosImpl::CommitStep(uint64_t index, uint64_t store_seq)
{
    assert(0 < index);
    assert(commited_index_ <= next_commited_index_);
    if (pending_index_.end() != pending_index_.find(index)) {
        assert(0 != pending_index_[index]);
        if (store_seq == pending_index_[index]) {
            pending_index_.erase(index);
            TryUpdateNextCommitedIndex();
        }
    }

    if (ins_map_.size() >= MAX_INS_SIZE && 
            index < commited_index_ &&
            pending_index_.end() == pending_index_.find(index)) {
        // gc paxos instance: only commited & non-pending one
        ins_map_.erase(index);
        logdebug("GC paxos instance index %" PRIu64, index);
    }

    commited_index_ = next_commited_index_;
}

std::tuple<uint64_t, std::unique_ptr<Message>>
PaxosImpl::ProduceRsp(
        const PaxosInstance* ins, 
        const Message& req_msg, 
        MessageType rsp_msg_type)
{
    assert(nullptr != ins);
    hassert(req_msg.logid() == logid_, 
            "req_msg.logid %" PRIu64 " logid_ %" PRIu64, 
            req_msg.logid(), logid_);
    hassert(req_msg.to_id() == selfid_, "type %d req_msg.to_id %" 
            PRIu64 " selfid_ %" PRIu64 "\n", 
            static_cast<int>(req_msg.type()), req_msg.to_id(), selfid_);

    uint64_t seq = 0;
    uint64_t prev_next_commited_index = 0;
    unique_ptr<Message> rsp_msg;

    switch (rsp_msg_type) {
    case MessageType::PROP:
        seq = ++store_seq_;
        rsp_msg = unique_ptr<Message>{new Message};
        assert(nullptr != rsp_msg);

        rsp_msg->set_type(MessageType::PROP);
        rsp_msg->set_proposed_num(ins->GetProposeNum());
        rsp_msg->set_peer_id(selfid_);
        rsp_msg->set_to_id(0);  // broad cast;
        break;
    case MessageType::PROP_RSP:
        rsp_msg = unique_ptr<Message>{new Message};
        assert(nullptr != rsp_msg);
        rsp_msg->set_type(MessageType::PROP_RSP);
        rsp_msg->set_proposed_num(req_msg.proposed_num());
        rsp_msg->set_peer_id(selfid_);
        rsp_msg->set_to_id(req_msg.peer_id());
        rsp_msg->set_promised_num(ins->GetPromisedNum());
        assert(rsp_msg->promised_num() >= rsp_msg->proposed_num());
        if (req_msg.proposed_num() == rsp_msg->promised_num()) {
            // promised 
            rsp_msg->set_accepted_num(ins->GetAcceptedNum());
            rsp_msg->set_accepted_value(ins->GetAcceptedValue());
        }

        // : store the state
        if (req_msg.proposed_num() == ins->GetPromisedNum()) {
            seq = ++store_seq_;
        }

        break;
    case MessageType::ACCPT:
        seq = ++store_seq_;
        rsp_msg = unique_ptr<Message>{new Message};
        assert(nullptr != rsp_msg);
        rsp_msg->set_type(MessageType::ACCPT);
        rsp_msg->set_proposed_num(ins->GetProposeNum());
        rsp_msg->set_peer_id(selfid_);
        rsp_msg->set_to_id(0); // broadcast
        rsp_msg->set_accepted_value(ins->GetAcceptedValue());
        break;
    case MessageType::ACCPT_RSP:
        seq = ++store_seq_;
        rsp_msg = unique_ptr<Message>{new Message};
        assert(nullptr != rsp_msg);
        rsp_msg->set_type(MessageType::ACCPT_RSP);
        rsp_msg->set_proposed_num(req_msg.proposed_num());
        rsp_msg->set_peer_id(selfid_);
        rsp_msg->set_to_id(req_msg.peer_id());
        rsp_msg->set_promised_num(ins->GetPromisedNum());
        rsp_msg->set_accepted_num(ins->GetAcceptedNum());

        // : store the sate
        if (req_msg.proposed_num() == ins->GetAcceptedNum()) {
            seq = ++store_seq_;
        }

        break;
    case MessageType::CHOSEN:
        // mark index as chosen
        if (MessageType::CHOSEN != req_msg.type()) {
            rsp_msg = unique_ptr<Message>{new Message};
            assert(nullptr != rsp_msg);
            rsp_msg->set_type(MessageType::CHOSEN);
            rsp_msg->set_proposed_num(req_msg.proposed_num());
            rsp_msg->set_peer_id(selfid_);
            rsp_msg->set_to_id(0); // broadcast
            rsp_msg->set_promised_num(ins->GetPromisedNum());
            rsp_msg->set_accepted_num(ins->GetAcceptedNum());
            if (rsp_msg->accepted_num() != req_msg.accepted_num()) {
                rsp_msg->set_accepted_value(ins->GetAcceptedValue());
            }
        }
        // else => no rsp

        // update next_commited_index_
        hassert(next_commited_index_ >= commited_index_, 
                "commited_index_ %" PRIu64 "next_commited_index_ %" PRIu64, 
                commited_index_, next_commited_index_);
        // TODO: fix
        // => Since we don't lock_guard db.Set operation, 
        //    chance that we will incomme situation like 
        assert(0 == seq);
        prev_next_commited_index = next_commited_index_;
        if (req_msg.index() > next_commited_index_) {
            TryUpdateNextCommitedIndex();
        }

        logdebug("index %" PRIu64 " commited_index %" PRIu64 
                " prev_next_commit_index %" PRIu64 
                " next_commited_index_ %" PRIu64, 
                req_msg.index(), commited_index_, prev_next_commited_index, 
                next_commited_index_);
        break;
    case MessageType::UNKOWN:
        if (MessageType::CHOSEN == req_msg.type()) {
            assert(req_msg.accepted_num() != ins->GetAcceptedNum());

            seq = ++store_seq_;
            // rsp_msg for self
            rsp_msg = unique_ptr<Message>{new Message};
            assert(nullptr != rsp_msg);
            rsp_msg->set_type(MessageType::CHOSEN);
            rsp_msg->set_proposed_num(ins->GetProposeNum());
            rsp_msg->set_peer_id(selfid_);
            rsp_msg->set_to_id(selfid_); // self-call after succ store hs;??
            rsp_msg->set_promised_num(ins->GetPromisedNum());
            rsp_msg->set_accepted_num(ins->GetAcceptedNum());
        }
        // else => ignore
        break;
    default:
        hassert(false, "%s rsp_msg_type %u", __func__, 
                static_cast<int>(rsp_msg_type));
        // do nothing
        break;
    }

    if (pending_index_.end() != pending_index_.find(req_msg.index())) {
        if (0 == seq) {
            seq = pending_index_[req_msg.index()];
        }
    }

    if (0 != seq) {
        assert(seq >= pending_index_[req_msg.index()]);
        pending_index_[req_msg.index()] = seq;
    }

    if (nullptr != rsp_msg) {
        rsp_msg->set_logid(req_msg.logid());
        rsp_msg->set_index(req_msg.index());
    }

    return make_tuple(seq, move(rsp_msg));
}

std::tuple<std::string, std::string> 
PaxosImpl::GetInfo(uint64_t index) const
{
    assert(0 < index);
    if (ins_map_.end() == ins_map_.find(index)) {
        return make_tuple<string, string>("null", "");
    }

    auto ins = ins_map_.at(index).get();
    assert(nullptr != ins);

    stringstream ss;
    ss << "LogId " << logid_ 
       << " State " << ins->GetState()
       << " ProposeNum " << ins->GetProposeNum() 
       << " PromisedNum " << ins->GetPromisedNum()
       << " AcceptedNum " << ins->GetAcceptedNum();
    return make_tuple(ss.str(), ins->GetAcceptedValue());
}

void PaxosImpl::TryUpdateNextCommitedIndex() 
{
    uint64_t prev_next_commited_index = next_commited_index_;
    for (auto next = next_commited_index_ + 1; 
            next <= max_index_; ++next) {
        assert(next > commited_index_);
        auto ins = GetInstance(next, false);
        assert(nullptr != ins);
        if (!ins->IsChosen() || 
                pending_index_.end() != pending_index_.find(next)) {
            break;
        }

        next_commited_index_ = next;
    }
    return ;
}

std::set<uint64_t> 
PaxosImpl::GetAllTimeoutIndex(const std::chrono::milliseconds timeout)
{
    set<uint64_t> timeout_ins;
    for (const auto& idx_ins_pair : ins_map_) {
        uint64_t index = idx_ins_pair.first;
        PaxosInstance* ins = idx_ins_pair.second.get();
        assert(0 < index);
        assert(nullptr != ins);

        if (!ins->IsChosen() && ins->IsTimeout(timeout)) {
            timeout_ins.insert(index);
        }
    }

    return timeout_ins;
}

} // namspace paxos


