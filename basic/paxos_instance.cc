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
        return 
    }

    // else
    rsp_votes[peer_id] = vote;
}

std::pair<int, int> countVotes(const std::map<uint64_t, bool>& votes)
{
    int true_cnt = 0;
    int false_cnt = 0;
    for (const auto v& : votes) {
        if (v.second) {
            ++true_cnt;
        } else {
            ++false_cnt;
        }
    }

    return make_pair(true_cnt, false_cnt);
}

}

namespace paxos {

enum {
    HANDLE_OK = 0, 
    HANDLE_SUCC = 1,
    HANDLE_RESET = 2, 

    HANDLE_IGNORE = 10, 
    HANDLE_ERROR = -1, 
};



MessageType PaxosInstance::StepProposer(const Message& msg)
{
    if (PropState::CHOSEN == prop_state_) {
        return MessageType::CHOSEN;
    }
    assert(PropState::NIL != prop_state_);

    PropState next_prop_state = PropState::NIL;
    switch (msg.type) {
    case MessageType::PROP_RSP:
        {
            assert(PropState::WAIT_PREPARE == prop_state_);
            next_prop_state = stepPrepareRsp(
                    msg.prop_num, msg.peer_id, 
                    msg.promised_num, msg.accepted_num, 
                    msg.accepted_value);
        }
        break;
    case MessageType::ACCPT_RSP:
        {
            assert(PropState::WAIT_ACCEPT == prop_state_);
            assert(nullptr == msg.accepted_num);
            assert(nullptr == msg.accepted_value);
            ret = stepAcceptRsp(msg.prop_num, 
                    msg.peer_id, msg.promied_num);
        }
        break;
    default:
        hassert(false, "%s msgtype %u", __func__, msg.type);
    };

    return updatePropState(next_prop_state);
    // TODO: 
    // - 0: nothing
    // - 1: store state or send msg
}

MessageType PaxosInstance::StepAcceptor(const Message& msg)
{
    if (PropState::CHOSEN == prop_state_) {
        return MessageType::CHOSEN;
    }

    MessageType rsp_msg_type = MessageType::UNKOWN;
    switch (msg.type) {
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
    }
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
            uint64_t next_proposed_num = prop_num_gen_.Next(promised_num_);
            auto new_state = beginPreparePhase(next_prop_num);
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
            asseert(PropState::WAIT_ACCEPT == new_state);
            next_prop_state = PropState::WAIT_ACCEPT;
            rsp_msg_type = MessageType::ACCPT;
        }
        break;
        break;
    default:
        hassert("invalid PropState %d", static_cast<int>(next_prop_state));
    }
    prop_state_ = next_prop_state; 
    return rsp_msg_type;
}

PropState PaxosInstance::beginPreparePhase(uint64_t next_proposed_num)
{
    assert(PROP_PREPARE == prop_state_);
    assert(false == chosen_);
    assert(next_proposed_num >= max_proposed_num_);

    bool reject = updatePromised(next_proposed_num);
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
    max_proposed_num_ = next_proposed_num;
    // TODO: 
    // 1. appendHardState
    // 2. appendMsg
    return PropState::WAIT_PREPARE;
}

PropState PaxosInstance::beginAcceptPhase()
{
    assert(PROP_ACCEPT == prop_state_);
    assert(false == chosen_);

    // max_accepted_hint_num_:
    // => TODO: how to tell, proposing_value_ is local prop or other peers ?
    bool reject = updateAccepted(max_proposed_num_, proposing_value_);
    if (reject) {
        return PropState::PREPARE;
    }

    assert(false == reject);

    rsp_votes_.clear();
    // TODO: appendMsg + appendHardState
    return PropState::WAIT_ACCEPT;
}

PropState PaxosInstance::stepPrepareRsp(
        uint64_t prop_num, 
        uint64_t peer_id, 
        uint64_t peer_promised_num, 
        uint64_t* peer_accepted_num, 
        const std::string* peer_accepted_value)
{
    assert(0 < peer_id);
    assert(0 < major_cnt_);
    assert(PropState::WAIT_PREPARE == prop_state_);
    if (prop_num != max_proposed_num_) {
        // ignore: prop num mis-match
        return PropState::WAIT_PREPARE;
    }

    // TODO: assert curr_rsp_votes_[peer_id] == prev_rsp_votes_[peer_id]
    updateRspVotes(peer_id, 
            max_proposed_num_ >= peer_promised_num, rsp_votes_);
    if (max_proposed_num_ >= peer_promised_num) {
        // peer promised
        if (nullptr != peer_accepted_num) {
            assert(nullptr != peer_accepted_value);
            if (*peer_accepted_num > max_accepted_hint_num_) {
                max_accepted_hint_num_ = *peer_accepted_num;
                proposing_value_ = *peer_accepted_value;
            }
        }
    }

    auto vote_res = countVotes(rsp_votes_);
    if (vote_res.second >= major_cnt_) {
        // rejected by majority
        return PropState::PREPARE;
        // vote_res.frist + 1 (including self-vote)
    } else if (vote_res.first + 1 >= major_cnt_) {
        return PropState::ACCEPT;
    }
    
    return PropState::WAIT_PREPARE;
}

int PaxosInstance::stepAcceptRsp(
        uint64_t prop_num, 
        uint64_t peer_id, 
        uint64_t peer_promised_num)
{
    assert(0 < peer_id);
    assert(0 < major_cnt_);
    assert(PropState::WAIT_ACCEPT == prop_state_);

    if (prop_num != max_proposed_num_) {
        // ignore: prop num mis-match
        return PropState::WAIT_ACCEPT;
    }

    updateRspVotes(peer_id, 
            max_proposed_num_ >= peer_promised_num, rsp_votes_);
    auto vote_res = countVotes(rsp_votes_);
    if (vote_res.second >= major_cnt_) {
        return PropState::PREPARE;
    } else if (vote_res.first+1 >= major_cnt_) {
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


