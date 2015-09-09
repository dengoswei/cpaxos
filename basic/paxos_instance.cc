#include "paxos_instance.h"

using namespace std;

namespace {

void updateRspVotes(
        uint64_t peer_id, bool vote, 
        std::map<uint64_t, bool>& rsp_votes)
{
    assert(0 < peer_id);

    if (rsp_votes.end() != rsp_votes.find(peer_id)) {
        assert(rsp_votes[peer_id] == vote);
        return ;
    }

    // else
    rsp_votes[peer_id] = vote;
}

std::tuple<int, int> countVotes(const std::map<uint64_t, bool>& votes)
{
    int true_cnt = 0;
    int false_cnt = 0;
    for (const auto& v : votes) {
        if (v.second) {
            ++true_cnt;
        } else {
            ++false_cnt;
        }
    }

    return make_tuple(true_cnt, false_cnt);
}

}

namespace paxos {

PaxosInstance::PaxosInstance(int major_cnt, uint64_t prop_num)
    : group_size_(major_cnt)
    , prop_num_gen_(prop_num)
{

}

int PaxosInstance::Propose(const std::string& proposing_value)
{
    if (PropState::NIL != prop_state_ || 0 != accepted_num_)  
    {
        // refuse to propose if
        // - prop in this instance before;
        // - instance have accepted other prop before
        return -1;
    }

    assert(PropState::NIL == prop_state_);
    prop_state_ = PropState::PREPARE;
    proposing_value_ = proposing_value;

    if (prop_num_gen_.Get() < promised_num_) {
        uint64_t prev_prop_num = prop_num_gen_.Get();
        prop_num_gen_.Next(promised_num_);
        assert(prev_prop_num < prop_num_gen_.Get());
    }
    assert(prop_num_gen_.Get() >= promised_num_);
    prop_state_ = beginPreparePhase();
    assert(PropState::WAIT_PREPARE == prop_state_);
    return 0;
}

MessageType PaxosInstance::Step(const Message& msg)
{
    assert(promised_num_ >= accepted_num_);
    if (PropState::CHOSEN == prop_state_) {
        return MessageType::CHOSEN;
    }

    MessageType rsp_msg_type = MessageType::UNKOWN;
    switch (msg.type) {
        // proposer
    case MessageType::PROP_RSP:
        {
            assert(PropState::WAIT_PREPARE == prop_state_);
            auto next_prop_state = stepPrepareRsp(
                    msg.prop_num, msg.peer_id, 
                    msg.promised_num, msg.accepted_num, 
                    msg.accepted_value);
            rsp_msg_type = updatePropState(next_prop_state);
        }
        break;
    case MessageType::ACCPT_RSP:
        {
            assert(PropState::WAIT_ACCEPT == prop_state_);
            assert(nullptr == msg.accepted_value);
            auto next_prop_state = stepAcceptRsp(msg.prop_num, 
                    msg.peer_id, msg.promised_num);
            rsp_msg_type = updatePropState(next_prop_state);
        }
        break;
    case MessageType::PROP:
        updatePromised(msg.prop_num);
        rsp_msg_type = MessageType::PROP_RSP;
        break;
    case MessageType::ACCPT:
        assert(nullptr != msg.accepted_value);
        updateAccepted(msg.prop_num, *msg.accepted_value);
        rsp_msg_type = MessageType::ACCPT_RSP;
        break;

    default:
        hassert(false, "%s msgtype %u", __func__, msg.type);
    };

    return rsp_msg_type;
}

MessageType PaxosInstance::updatePropState(PropState next_prop_state)
{
    MessageType rsp_msg_type = MessageType::NOOP;

    switch (next_prop_state) {
    case PropState::WAIT_PREPARE:
    case PropState::WAIT_ACCEPT:
        break;
    case PropState::CHOSEN:
        rsp_msg_type = MessageType::CHOSEN;
        break;
    case PropState::PREPARE:
        {
            prop_num_gen_.Next(promised_num_);
            assert(prop_num_gen_.Get() >= promised_num_);
            auto new_state = beginPreparePhase();
            assert(PropState::WAIT_PREPARE == new_state);
            next_prop_state = PropState::WAIT_PREPARE;
            rsp_msg_type = MessageType::PROP; 
        }
        break;
    case PropState::ACCEPT:
        {
            auto new_state = beginAcceptPhase();
            if (PropState::PREPARE == new_state)
            {
                return updatePropState(PropState::PREPARE);
            }
            assert(PropState::WAIT_ACCEPT == new_state);
            next_prop_state = PropState::WAIT_ACCEPT;
            rsp_msg_type = MessageType::ACCPT;
        }
        break;
    default:
        hassert(false, "invalid PropState %d", static_cast<int>(next_prop_state));
    }
    prop_state_ = next_prop_state; 
    return rsp_msg_type;
}

PaxosInstance::PropState PaxosInstance::beginPreparePhase()
{
    assert(PropState::PREPARE == prop_state_);
    bool reject = updatePromised(prop_num_gen_.Get());
    if (reject) {
        return PropState::PREPARE;
    }

    assert(false == reject);
    if (max_accepted_hint_num_ < accepted_num_) {
        max_accepted_hint_num_ = accepted_num_;
        proposing_value_ = accepted_value_;
    }

    // ignore rsp_votes_[self_id_]
    rsp_votes_.clear();
    return PropState::WAIT_PREPARE;
}

PaxosInstance::PropState PaxosInstance::beginAcceptPhase()
{
    assert(PropState::ACCEPT == prop_state_);
    bool reject = updateAccepted(prop_num_gen_.Get(), proposing_value_);
    if (reject) {
        return PropState::PREPARE;
    }

    assert(false == reject);
    rsp_votes_.clear();
    return PropState::WAIT_ACCEPT;
}

PaxosInstance::PropState PaxosInstance::stepPrepareRsp(
        uint64_t prop_num, 
        uint64_t peer_id, 
        uint64_t peer_promised_num, 
        uint64_t peer_accepted_num, 
        const std::string* peer_accepted_value)
{
    assert(0 < peer_id);
    assert(0 < group_size_);
    assert(PropState::WAIT_PREPARE == prop_state_);
    uint64_t max_proposed_num = prop_num_gen_.Get();
    if (prop_num != max_proposed_num) {
        // ignore: prop num mis-match
        return PropState::WAIT_PREPARE;
    }

    // TODO: assert curr_rsp_votes_[peer_id] == prev_rsp_votes_[peer_id]
    updateRspVotes(peer_id, 
            max_proposed_num >= peer_promised_num, rsp_votes_);
    if (max_proposed_num >= peer_promised_num) {
        // peer promised
        if (nullptr != peer_accepted_value) {
            if (peer_accepted_num > max_accepted_hint_num_) {
                max_accepted_hint_num_ = peer_accepted_num;
                proposing_value_ = *peer_accepted_value;
            }
        }
    }

    int promise_cnt = 0;
    int reject_cnt = 0;
    tie(promise_cnt, reject_cnt) = countVotes(rsp_votes_);
    if (reject_cnt >= group_size_) {
        // rejected by majority
        return PropState::PREPARE;
        // vote_res.frist + 1 (including self-vote)
    } else if (promise_cnt + 1 >= group_size_) {
        return PropState::ACCEPT;
    }
    
    return PropState::WAIT_PREPARE;
}

PaxosInstance::PropState PaxosInstance::stepAcceptRsp(
        uint64_t prop_num, 
        uint64_t peer_id, 
        uint64_t peer_promised_num)
{
    assert(0 < peer_id);
    assert(0 < group_size_);
    assert(PropState::WAIT_ACCEPT == prop_state_);
    uint64_t max_proposed_num = prop_num_gen_.Get();
    if (prop_num != max_proposed_num) {
        // ignore: prop num mis-match
        return PropState::WAIT_ACCEPT;
    }

    updateRspVotes(peer_id, 
            max_proposed_num >= peer_promised_num, rsp_votes_);

    int accept_cnt = 0;
    int reject_cnt = 0;
    tie(accept_cnt, reject_cnt) = countVotes(rsp_votes_);
    if (reject_cnt >= group_size_) {
        return PropState::PREPARE;
    } else if (accept_cnt+1 >= group_size_) {
        return PropState::CHOSEN;
    }

    return PropState::WAIT_ACCEPT;
}


bool PaxosInstance::updatePromised(uint64_t prop_num)
{
    if (promised_num_ > prop_num) {
        // reject
        return false;
    }

    promised_num_ = prop_num;
    return true;
}

bool PaxosInstance::updateAccepted(
        uint64_t prop_num, const std::string& prop_value)
{
    if (promised_num_ > prop_num) {
        // reject
        return false;
    }

    promised_num_ = prop_num;
    accepted_num_ = prop_num;
    accepted_value_ = prop_value;
    return true;
}



} // namespace paxos


