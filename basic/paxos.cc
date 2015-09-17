#include "paxos.h"


using namespace std;

namespace {

using namespace paxos;

std::unique_ptr<PaxosInstance> buildPaxosInstance(
        size_t peer_set_size, uint64_t selfid, uint64_t prop_cnt)
{
    const int major_cnt = static_cast<int>(peer_set_.size())/2 + 1;
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
    auto hs = unique_ptr<proto::HardState>{new proto::HardState};
    assert(nullptr != hs);

    hs->set_index();
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

    {
        lock_guard<mutex> lock(paxos_mutex_);
        if (max_index_ != commited_index_) {
            return make_tuple(-1, 0);
        }

        auto new_ins = buildPaxosInstance(peer_set_.size(), selfid_, 0);
        assert(nullptr != new_ins);
        int ret = new_ins->Propose(proposing_value);
        assert(0 == ret); // always ret 0 on new PaxosInstance 
        
        uint64_t new_index = max_index_ + 1;
        assert(ins_map_.end() == ins_map_.find(max_index_+1));
        ins_map_[max_index_+1] = move(new_ins);
        assert(nullptr == new_ins);
        ++max_index_;

        tie(hs, rsp_msg) = produceRsp(
                new_index, ins.get(), Message{}, MessageType::PROP);
    }

    while (true) {
        int ret = callback(index, hs, rsp_msg);
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

    bool update_index = false;
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
            update_index = true;
        }
    }

    // 2.
    int ret = callback(index, hs, rsp_msg);
    if (0 != ret) {
        // error case
        return ret;
    }

    assert(0 == ret);
    if (false == update_index) {
        return 0;
    }

    // 3.
    {
        lock_guard<mutex> lock(paxos_mutex_);
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
        break;
    case MessageType::PROP_RSP:
        rsp_msg = unique_ptr<Message>{new Message};
        assert(nullptr != rsp_msg);
        rsp_msg->type = MessageType::PROP_RSP;
        rsp_msg->prop_num = req_msg.prop_num;
        rsp_msg->peer_id = selfid_;
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
        rsp_msg->accepted_value = hs->accepted_value();
        break;
    case MessageType::ACCPT_RSP:
        rsp_msg = unique_ptr<Message>{new Message};
        assert(nullptr != rsp_msg);
        rsp_msg->type = MessageType::ACCPT_RSP;
        rsp_msg->prop_num = req_msg.prop_num;
        rsp_msg->peer_id = selfid_;
        rsp_msg->promised_num = ins->GetPromisedNum();
        rsp_msg->accepted_num = ins->GetAcceptedNum();
        break;
    case MessageType::CHOSEN:
        // mark index as chosen
        chosen_set_.insert(index);
        rsp_msg = unique_ptr<Message>{new Message};
        assert(nullptr != rsp_msg);
        rsp_msg->type = MessageType::CHOSEN;
        rsp_msg->prop_num = req_msg.prop_num;
        rsp_msg->peer_id = selfid_;
        rsp_msg->promised_num = ins->GetPromisedNum();
        rsp_msg->accepted_num = ins->GetAcceptedNum(); 
        rsp_msg->accepted_value = ins->GetAcceptedValue();

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
    default:
        hassert(false, "%s rsp_msg_type %u", __func__, 
                static_cast<int>(rsp_msg_type));
        // do nothing
        break;
    }

    return make_tuple(hs, rsp_msg);
}

int Paxos::Wait(uint64_t index)
{
    unique_lock<mutex> lock(paxos_mutex_);
    paxos_cv_.wait(lock, []{ return index <= commited_index_; });

    return 0;
}


} // namspace paxos


