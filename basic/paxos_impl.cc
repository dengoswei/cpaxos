#include "paxos_impl.h"
#include "paxos_instance.h"
#include "utils.h"
#include "paxos.pb.h"


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

std::unique_ptr<proto::HardState> 
createHardState(uint64_t index, const PaxosInstance* ins)
{
    assert(0 < index);
    assert(nullptr != ins);
    auto hs = unique_ptr<proto::HardState>{new proto::HardState{}};
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
        std::unique_ptr<PaxosInstance>&& proposing_ins)
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
        std::unique_ptr<PaxosInstance>&& proposing_ins)
{
    assert(index == proposing_index_);
    assert(0 != proposing_index_);
    assert(nullptr != proposing_ins);
    assert(ins_map_.end() == ins_map_.find(index));

    ins_map_[index] = move(proposing_ins);
    assert(nullptr == proposing_ins);
    max_index_ = max(max_index_, index);
    pending_index_.erase(index);
    proposing_index_ = 0;
}

//std::tuple<int, uint64_t> 
//PaxosImpl::Propose(
//        const std::string& proposing_value, Callback callback)
//{
//    unique_ptr<proto::HardState> hs;
//    unique_ptr<Message> rsp_msg;
//
//    // limit only one active propose
//    auto new_index = NextProposingIndex();
//    if (0 == new_index) {
//        return make_tuple(-1, 0);
//    }
//    assert(ins_map_.end() == ins_map_.find(new_index));
//
//    // TODO
//    auto new_ins = buildPaxosInstance(peer_set_.size(), selfid_, 0);
//    assert(nullptr != new_ins);
//    int ret = new_ins->Propose(proposing_value);
//    assert(0 == ret); // always ret 0 on new PaxosInstance 
//
//    Message fake_msg;
//    fake_msg.to_id = selfid_;
//    tie(hs, rsp_msg) = ProduceRsp(
//            new_index, new_ins.get(), fake_msg, MessageType::PROP);
//    ret = callback(new_index, hs, rsp_msg);
//    if (0 != ret) {
//        DiscardProposingInstance(new_index, move(new_ins));
//        assert(nullptr == new_ins);
//        assert(0 == proposing_index_);
//        return make_tuple(-2, 0);
//    }
//
//    CommitProposingInstance(new_index, move(new_ins));
//    assert(nullptr == new_ins);
//    assert(0 == proposing_index_);
//    assert(new_index <= max_index_);
//    
//    return make_tuple(0, new_index);
//}

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

void PaxosImpl::CommitStep(uint64_t index)
{
    assert(0 < index);
    assert(index != proposing_index_);
    assert(commited_index_ <= next_commited_index_);
    commited_index_ = next_commited_index_;
    pending_index_.erase(index);
}

//int PaxosImpl::Step(
//        uint64_t index, const Message& msg, Callback callback)
//{
//    assert(0 < index);
//    unique_ptr<proto::HardState> hs;
//    unique_ptr<Message> rsp_msg;
//
//    // 1.
//    // check index with commited_index_, max_index_
//    assert(commited_index_ <= max_index_);
//    if (IsChosen(index)) {
//        // msg on commited_index
//        return 1;
//    }
//
//    PaxosInstance* ins = GetInstance(index);
//    if (nullptr == ins) {
//        return -1;
//    }
//
//    auto rsp_msg_type = ins->Step(msg);
//    tie(hs, rsp_msg) = ProduceRsp(index, ins, msg, rsp_msg_type);
//
//    // 2.
//    int ret = callback(index, hs, rsp_msg);
//    if (0 != ret) {
//        // error case: TODO
//        // => IMPT: in-case of store hs failed ???
//        //    => hs re-restore need be done!!!!
//        return ret;
//    }
//
//    // 3.
//    assert(0 == ret);
//    CommitStep(index);
//    logdebug("commited_index_ %" PRIu64 " next_commited_index_ %" 
//            PRIu64 "\n", commited_index_, next_commited_index_);
//    // TODO: store commited_index_ ?
//    return 0;
//}

std::tuple<
    std::unique_ptr<proto::HardState>, 
    std::unique_ptr<Message>> 
PaxosImpl::ProduceRsp(
        uint64_t index, 
        const PaxosInstance* ins, 
        const Message& req_msg, 
        MessageType rsp_msg_type)
{
    assert(nullptr != ins);
    hassert(req_msg.to_id == selfid_, "type %d req_msg.to_id %" 
            PRIu64 " selfid_ %" PRIu64 "\n", 
            static_cast<int>(req_msg.type), req_msg.to_id, selfid_);

    unique_ptr<proto::HardState> hs;
    unique_ptr<Message> rsp_msg;

    switch (rsp_msg_type) {
    case MessageType::PROP:
        hs = createHardState(index, ins);
        assert(nullptr != hs);

        rsp_msg = unique_ptr<Message>{new Message};
        assert(nullptr != rsp_msg);
        rsp_msg->type = MessageType::PROP;
        rsp_msg->prop_num = hs->proposed_num();
        rsp_msg->peer_id = selfid_;
        rsp_msg->to_id = 0; // broad cast;
        break;
    case MessageType::PROP_RSP:
        rsp_msg = unique_ptr<Message>{new Message};
        assert(nullptr != rsp_msg);
        rsp_msg->type = MessageType::PROP_RSP;
        rsp_msg->prop_num = req_msg.prop_num;
        rsp_msg->peer_id = selfid_;
        rsp_msg->to_id = req_msg.peer_id;
        rsp_msg->promised_num = ins->GetPromisedNum();
        assert(rsp_msg->promised_num >= rsp_msg->prop_num);
        if (req_msg.prop_num == rsp_msg->promised_num) {
            // promised 
            rsp_msg->accepted_num = ins->GetAcceptedNum();
            rsp_msg->accepted_value = ins->GetAcceptedValue();
        }
        break;
    case MessageType::ACCPT:
        hs = createHardState(index, ins);
        assert(nullptr != hs);

        rsp_msg = unique_ptr<Message>{new Message};
        assert(nullptr != rsp_msg);
        rsp_msg->type = MessageType::ACCPT;
        rsp_msg->prop_num = hs->proposed_num();
        rsp_msg->peer_id = selfid_;
        rsp_msg->to_id = 0; // broadcast
        rsp_msg->accepted_value = hs->accepted_value();
        break;
    case MessageType::ACCPT_RSP:
        rsp_msg = unique_ptr<Message>{new Message};
        assert(nullptr != rsp_msg);
        rsp_msg->type = MessageType::ACCPT_RSP;
        rsp_msg->prop_num = req_msg.prop_num;
        rsp_msg->peer_id = selfid_;
        rsp_msg->to_id = req_msg.peer_id;
        rsp_msg->promised_num = ins->GetPromisedNum();
        rsp_msg->accepted_num = ins->GetAcceptedNum();
        break;
    case MessageType::CHOSEN:
        // mark index as chosen
        if (MessageType::CHOSEN != req_msg.type) {
            rsp_msg = unique_ptr<Message>{new Message};
            assert(nullptr != rsp_msg);
            rsp_msg->type = MessageType::CHOSEN;
            rsp_msg->prop_num = req_msg.prop_num;
            rsp_msg->peer_id = selfid_;
            rsp_msg->to_id = 0; // broadcast
            rsp_msg->promised_num = ins->GetPromisedNum();
            rsp_msg->accepted_num = ins->GetAcceptedNum(); 
            if (rsp_msg->accepted_num != req_msg.accepted_num) {
                rsp_msg->accepted_value = ins->GetAcceptedValue();
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
        if (MessageType::CHOSEN == req_msg.type) {
            assert(req_msg.accepted_num != ins->GetAcceptedNum());

            // else=> store
            hs = createHardState(index, ins);
            assert(nullptr != hs);

            // rsp_msg for self
            rsp_msg = unique_ptr<Message>{new Message};
            assert(nullptr != rsp_msg);
            rsp_msg->type = MessageType::CHOSEN;
            rsp_msg->prop_num = ins->GetProposeNum(); 
            rsp_msg->peer_id = selfid_;
            rsp_msg->to_id = selfid_; // self-call after succ store hs;
            rsp_msg->promised_num = ins->GetPromisedNum();
            rsp_msg->accepted_num = ins->GetAcceptedNum();
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
        if (nullptr != hs) {
            hs = createHardState(index, ins);
        }
    }

    if (nullptr != hs) {
        pending_index_.insert(index); 
    }

    return make_tuple(move(hs), move(rsp_msg));
}

} // namspace paxos


