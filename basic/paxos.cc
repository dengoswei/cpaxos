#include "paxos.h"
#include "paxos_instance.h"
#include "utils.h"
#include "paxos.pb.h"


using namespace std;

namespace {

using namespace paxos;

std::unique_ptr<PaxosInstance> buildPaxosInstance(
        size_t peer_set_size, uint64_t selfid, uint64_t prop_cnt)
{
    const int major_cnt = static_cast<int>(peer_set_size)/2 + 1;
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

Paxos::Paxos(uint64_t selfid)
    : selfid_(selfid)
{

}

std::tuple<int, uint64_t> 
    Paxos::Propose(const std::string& proposing_value, Callback callback)
{
    unique_ptr<proto::HardState> hs;
    unique_ptr<Message> rsp_msg;

    uint64_t new_index = 0;
    {
        lock_guard<mutex> lock(paxos_mutex_);
        if (max_index_ != commited_index_) {
            return make_tuple(-1, 0);
        }

        auto new_ins = buildPaxosInstance(peer_set_.size(), selfid_, 0);
        assert(nullptr != new_ins);
        int ret = new_ins->Propose(proposing_value);
        assert(0 == ret); // always ret 0 on new PaxosInstance 
        
        new_index = max_index_ + 1;
        assert(ins_map_.end() == ins_map_.find(max_index_+1));
        ins_map_[max_index_+1] = move(new_ins);
        assert(nullptr == new_ins);
        ++max_index_;

        Message fake_msg;
        fake_msg.to_id = selfid_;
        tie(hs, rsp_msg) = produceRsp(
                new_index, ins_map_[new_index].get(), fake_msg, MessageType::PROP);
    }

    assert(0 < new_index);
    while (true) {
        int ret = callback(new_index, hs, rsp_msg);
        if (0 == ret) {
            break;
        }

        // TODO: usleep maybe ?
    }

    return make_tuple(0, max_index_);
}


int Paxos::Step(uint64_t index, const Message& msg, Callback callback)
{
    unique_ptr<proto::HardState> hs;
    unique_ptr<Message> rsp_msg;

    bool update = false;
    // 1.
    {
        lock_guard<mutex> lock(paxos_mutex_);
        // check index with commited_index_, max_index_
        assert(commited_index_ <= max_index_);
        if (index <= commited_index_) {
            // msg on commited_index
            return 1;
        }

        if (ins_map_.end() == ins_map_.find(index)) {
            // need build a new paxos instance
            auto new_ins = 
                buildPaxosInstance(peer_set_.size(), selfid_, 0);
            assert(nullptr != new_ins);
            ins_map_[index] = move(new_ins);
            assert(nullptr == new_ins);
            max_index_ = max(max_index_, index);
        }

        PaxosInstance* ins = ins_map_[index].get();
        assert(nullptr != ins);

        auto rsp_msg_type = ins->Step(msg);
        tie(hs, rsp_msg) = produceRsp(index, ins, msg, rsp_msg_type);
        if (commited_index_ < next_commited_index_) {
            update = true;
        }

        if (pending_hs_.end() != pending_hs_.find(index)) {
            if (nullptr != hs) {
                hs = createHardState(index, ins);
            }
            assert(nullptr != hs);
            update = true;
        }
    }

    // 2.
    int ret = callback(index, hs, rsp_msg);
    if (0 != ret) {
        if (nullptr != hs && false == update) {
            lock_guard<mutex> lock(paxos_mutex_);
            pending_hs_.insert(index);
        }
        // error case: TODO
        // => IMPT: in-case of store hs failed ???
        //    => hs re-restore need be done!!!!
        return ret;
    }


    assert(0 == ret);
    if (false == update) {
        return 0;
    }

    // 3.
    {
        lock_guard<mutex> lock(paxos_mutex_);
        if (pending_hs_.end() != pending_hs_.find(index)) {
            pending_hs_.erase(index);
        }

        // double check
        if (commited_index_ >= next_commited_index_) {
            return 0;
        }

        commited_index_ = next_commited_index_;
        logdebug("commited_index_ %" PRIu64 " next_commited_index_ %" 
                PRIu64 "\n", commited_index_, next_commited_index_);
        // TODO: store commited_index_ ?
    }

    // 3. notify_all under unlock!
    paxos_cv_.notify_all();
    return 0;
}

std::tuple<
    std::unique_ptr<proto::HardState>, 
    std::unique_ptr<Message>> 
Paxos::produceRsp(
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

    return make_tuple(move(hs), move(rsp_msg));
}

int Paxos::Wait(uint64_t index)
{
    unique_lock<mutex> lock(paxos_mutex_);
    if (index <= commited_index_) {
        return 0;
    }

    paxos_cv_.wait(lock, [index, this]{ return index <= commited_index_; });
    return 0;
}


} // namspace paxos


