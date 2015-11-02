#include <sstream>
#include "paxos_impl.h"
#include "paxos_instance.h"
#include "utils.h"


using namespace std;

namespace {

using namespace paxos;

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

std::unique_ptr<HardState> 
createHardState(uint64_t index, const PaxosInstance* ins)
{
    assert(0 < index);
    assert(nullptr != ins);
    auto hs = unique_ptr<HardState>{new HardState{}};
    assert(nullptr != hs);

    hs->set_index(index);
    hs->set_proposed_num(ins->GetProposeNum());
    hs->set_promised_num(ins->GetPromisedNum());
    hs->set_accepted_num(ins->GetAcceptedNum());
    hs->set_accepted_value(ins->GetAcceptedValue());
    return hs;
}


} // namespace


namespace paxos {

PaxosImpl::PaxosImpl(uint64_t selfid, uint64_t group_size)
    : selfid_(selfid)
    , group_size_(group_size)
{
    assert(0 < selfid_);
    assert(selfid_ <= group_size_);
}

PaxosImpl::~PaxosImpl() = default;

uint64_t PaxosImpl::NextProposingIndex()
{
    if (max_index_ != commited_index_ || 0 != proposing_index_) {
        return 0;
    }
    
    assert(0 == proposing_index_);
    proposing_index_ = max_index_ + 1;
    return proposing_index_;
}

std::unique_ptr<PaxosInstance>
PaxosImpl::BuildNewPaxosInstance()
{
    return buildPaxosInstance(group_size_, selfid_, 0);
}

void PaxosImpl::DiscardProposingInstance(
        const uint64_t index, 
        std::unique_ptr<PaxosInstance> proposing_ins)
{
    assert(index == proposing_index_);
    assert(0 != proposing_ins);
    assert(nullptr != proposing_ins);
    assert(ins_map_.end() == ins_map_.find(index));

    pending_index_.erase(index);
    proposing_index_ = 0;
}

void PaxosImpl::CommitProposingInstance(
        const uint64_t index, 
        const uint64_t store_seq, 
        std::unique_ptr<PaxosInstance>&& proposing_ins)
{
    assert(index == proposing_index_);
    assert(0 != proposing_index_);
    assert(nullptr != proposing_ins);
    assert(ins_map_.end() == ins_map_.find(index));
    assert(0 != store_seq);

    ins_map_[index] = move(proposing_ins);
    assert(nullptr == proposing_ins);
    max_index_ = max(max_index_, index);

    if (pending_index_.end() != pending_index_.find(index)) {
        assert(0 != pending_index_[index]);
        if (store_seq == pending_index_[index]) {
            pending_index_.erase(index);
        }
    }

    proposing_index_ = 0;
}

PaxosInstance* PaxosImpl::GetInstance(uint64_t index)
{
    assert(0 < index);
    if (IsProposing(index)) {
        return nullptr;
    }

    assert(0 == proposing_index_);
    if (ins_map_.end() == ins_map_.find(index)) {
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
    assert(index != proposing_index_);
    assert(commited_index_ <= next_commited_index_);
    commited_index_ = next_commited_index_;
    if (pending_index_.end() != pending_index_.find(index)) {
        assert(0 != pending_index_[index]);
        if (store_seq == pending_index_[index]) {
            pending_index_.erase(index);
        }
    }
}

std::tuple<uint64_t, std::unique_ptr<Message>>
PaxosImpl::ProduceRsp(
        uint64_t index, 
        const PaxosInstance* ins, 
        const Message& req_msg, 
        MessageType rsp_msg_type)
{
    assert(nullptr != ins);
    hassert(req_msg.to_id() == selfid_, "type %d req_msg.to_id %" 
            PRIu64 " selfid_ %" PRIu64 "\n", 
            static_cast<int>(req_msg.type()), req_msg.to_id(), selfid_);

    uint64_t seq = 0;
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
        rsp_msg = unique_ptr<Message>{new Message};
        assert(nullptr != rsp_msg);
        rsp_msg->set_type(MessageType::ACCPT_RSP);
        rsp_msg->set_proposed_num(req_msg.proposed_num());
        rsp_msg->set_peer_id(selfid_);
        rsp_msg->set_to_id(req_msg.peer_id());
        rsp_msg->set_promised_num(ins->GetPromisedNum());
        rsp_msg->set_accepted_num(ins->GetAcceptedNum());
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
        if (index > next_commited_index_) {
            chosen_set_.insert(index);
            for (auto next = next_commited_index_ + 1; 
                    next <= max_index_; ++next) {
                if (0 == chosen_set_.count(next)) {
                    break;
                }
                next_commited_index_ = next;
                chosen_set_.erase(next);
            }
        }

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

    if (pending_index_.end() != pending_index_.find(index)) {
        if (0 == seq) {
            seq = pending_index_[index];
        }
    }

    if (0 != seq) {
        assert(seq >= pending_index_[index]);
        pending_index_[index] = seq;
    }

    if (nullptr != rsp_msg) {
        rsp_msg->set_index(index);
    }

    return make_tuple(seq, move(rsp_msg));
}

std::tuple<std::string, std::string> PaxosImpl::GetInfo(uint64_t index)
{
    assert(0 < index);
    if (ins_map_.end() == ins_map_.find(index)) {
        return make_tuple<string, string>("null", "");
    }

    auto ins = ins_map_[index].get();
    assert(nullptr != ins);

    stringstream ss;
    ss << "State " << ins->GetState()
       << " ProposeNum " << ins->GetProposeNum() 
       << " PromisedNum " << ins->GetPromisedNum()
       << " AcceptedNum " << ins->GetAcceptedNum();
    return make_tuple(ss.str(), ins->GetAcceptedValue());
}

} // namspace paxos

