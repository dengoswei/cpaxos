#include "paxos_instance.h"

using namespace std;

#define PROP_TIMEOUT_TICK 100 // 100ms ?

namespace {

std::chrono::milliseconds ACTIVE_TIME_OUT = std::chrono::milliseconds{100};

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


PaxosInstanceImpl::PaxosInstanceImpl(int major_cnt, uint64_t prop_num)
    : major_cnt_(major_cnt)
    , prop_num_gen_(prop_num)
    , active_proposer_time_(chrono::system_clock::now())
{

}

PaxosInstanceImpl::PaxosInstanceImpl(
        int major_cnt, 
        uint64_t prop_num, 
        uint64_t promised_num, 
        uint64_t accepted_num, 
        const std::string& accepted_value, 
        PropState prop_state, 
        uint32_t store_seq)
    : major_cnt_(major_cnt)
    , prop_num_gen_(prop_num)
    , prop_state_(prop_state)
    , promised_num_(promised_num)
    , accepted_num_(accepted_num)
    , accepted_value_(accepted_value)
    , active_proposer_time_(chrono::system_clock::now())
    , store_seq_(store_seq)
{

}


MessageType PaxosInstanceImpl::step(const Message& msg)
{
    assert(promised_num_ >= accepted_num_);
    if (PropState::CHOSEN == prop_state_) {
        return MessageType::CHOSEN;
    }

    MessageType rsp_msg_type = MessageType::UNKOWN;
    switch (msg.type()) {
        // proposer
    case MessageType::PROP_RSP:
        {
            if (PropState::WAIT_PREPARE != prop_state_) {
                logdebug("msgtype::PROP_RSP but instance in stat %d", 
                        static_cast<int>(prop_state_));
                break;
            }

            assert(PropState::WAIT_PREPARE == prop_state_);
            auto next_prop_state = stepPrepareRsp(
                    msg.proposed_num(), msg.from(), 
                    msg.promised_num(), msg.accepted_num(), 
                    msg.accepted_value());
            rsp_msg_type = updatePropState(next_prop_state);
            logdebug("%s next_prop_state %d rsp_msg_type %d", 
                    __func__, static_cast<int>(next_prop_state), 
                    static_cast<int>(rsp_msg_type));
        }
        break;
    case MessageType::ACCPT_RSP:
    case MessageType::FAST_ACCPT_RSP:
        {
            if (PropState::WAIT_ACCEPT != prop_state_) {
                logdebug("msgtype::ACCPT_RSP but instance in stat %d", 
                        static_cast<int>(prop_state_));
                break;
            }

            assert(PropState::WAIT_ACCEPT == prop_state_);
            assert(true == msg.accepted_value().empty());
            auto next_prop_state = stepAcceptRsp(
                    msg.proposed_num(), msg.from(), msg.accepted_num(), 
                    MessageType::FAST_ACCPT_RSP == msg.type());
            rsp_msg_type = updatePropState(next_prop_state);
            logdebug("%s next_prop_state %d rsp_msg_type %d", 
                    __func__, 
                    static_cast<int>(next_prop_state), 
                    static_cast<int>(rsp_msg_type));
        }
        break;
    case MessageType::PROP:
        updatePromised(msg.proposed_num());
        rsp_msg_type = MessageType::PROP_RSP;
        break;
    case MessageType::ACCPT:
    case MessageType::FAST_ACCPT:
        {
            bool fast_accept = MessageType::FAST_ACCPT == msg.type();
            updateAccepted(
                    msg.proposed_num(), 
                    msg.accepted_value(), fast_accept);
            rsp_msg_type = fast_accept ? 
                MessageType::FAST_ACCPT_RSP : MessageType::ACCPT_RSP;
        }
        break;

    case MessageType::CHOSEN:
        {
            assert(PropState::CHOSEN != getPropState());
            if (msg.accepted_num() == accepted_num_) {
                // mark as chosen
                rsp_msg_type = updatePropState(PropState::CHOSEN);
                break;
            }

            auto max_prop_num = max(
                    getProposeNum(), getPromisedNum());
            max_prop_num = max(max_prop_num, msg.proposed_num());
            prop_num_gen_.Update(max_prop_num);

            // self promised
            assert(false == updatePromised(getProposeNum()));
            // self accepted
            assert(false == updateAccepted(
                        getProposeNum(), 
                        msg.accepted_value(), false));
            clearStrictPropFlag();
            updatePropState(PropState::CHOSEN);
            hassert(getProposeNum() >= msg.proposed_num(), 
                    "%" PRIu64 " %" PRIu64 
                    " msg.from %" PRIu64 " to %" PRIu64, 
                    getProposeNum(), msg.proposed_num(), 
                    msg.from(), msg.to());
            assert(getProposeNum() == getPromisedNum());
            assert(getProposeNum() == getAcceptedNum());
            assert(false == getStrictPropFlag());
            assert(0 != getPendingSeq());
        }
        break;

    // propose
    case MessageType::BEGIN_PROP:
        {
            assert(0ull == msg.proposed_num());
            if (0ull != getPromisedNum()) {
                logerr("CONFLICT selfid %" PRIu64 " index %" PRIu64 
                        " promised_num %" PRIu64, 
                        msg.to(), msg.index(), getPromisedNum());
                break;
            }

            assert(PropState::NIL == prop_state_);
            assert(0ull == getPromisedNum());
            assert(0ull == getAcceptedNum());
            setStrictPropFlag(); // mark as strict prop

            auto next_prop_state = 
                stepBeginPropose(
                        msg.proposed_num(), msg.accepted_value());
            assert(PropState::PREPARE == next_prop_state);
            rsp_msg_type = updatePropState(next_prop_state);
            assert(MessageType::PROP == rsp_msg_type);
            assert(getProposeNum() == getPromisedNum());
            assert(0ull == getAcceptedNum());
            assert(true == getStrictPropFlag());
            assert(0 != getPendingSeq());
        }
        break;
    case MessageType::TRY_PROP:
        {
            auto next_prop_state = stepTryPropose(msg.proposed_num());
            assert(PropState::PREPARE == next_prop_state);
            rsp_msg_type = updatePropState(next_prop_state);
            assert(PropState::WAIT_PREPARE == prop_state_);
        }
        break;
    case MessageType::BEGIN_FAST_PROP:
        {
            assert(0ull == msg.proposed_num());
            if (0ull != getPromisedNum()) {
                logerr("CONFLICT selfid %" PRIu64 " index %" PRIu64
                        " promised_num %" PRIu64, 
                        msg.to(), msg.index(), getPromisedNum());
                break;
            }

            assert(PropState::NIL == prop_state_);
            assert(0ull == getPromisedNum());
            assert(0ull == getAcceptedNum());
            setStrictPropFlag(); // mark as strict prop

            // => skip prepare phase 
            auto next_prop_state = 
                stepBeginPropose(
                        msg.proposed_num(), msg.accepted_value());
            assert(PropState::PREPARE == next_prop_state);
            rsp_msg_type = updatePropState(next_prop_state);
            assert(MessageType::PROP == rsp_msg_type);

            // => jump into accept phase(so call fast prop)
            assert(0ull == getAcceptedNum());
            rsp_msg_type = updatePropState(PropState::ACCEPT);
            assert(MessageType::ACCPT == rsp_msg_type);
            assert(getProposeNum() == getPromisedNum());
            assert(getProposeNum() == getAcceptedNum());
            // assert(proposing_value_ == getAcceptedValue());
            assert(true == getStrictPropFlag());
            assert(0 != getPendingSeq());
            // set rsp_msg_type as fast_accpt
            rsp_msg_type = MessageType::FAST_ACCPT;
        }
        break;

    default:
        hassert(false, "%s msgtype %u", __func__, msg.type());
    };

    return rsp_msg_type;
}

MessageType 
PaxosInstanceImpl::updatePropState(PropState next_prop_state)
{
    MessageType rsp_msg_type = MessageType::NOOP;
    prop_state_ = next_prop_state;
    switch (prop_state_) {
    case PropState::WAIT_PREPARE:
    case PropState::WAIT_ACCEPT:
        break;
    case PropState::CHOSEN:
        rsp_msg_type = MessageType::CHOSEN;
        break;
    case PropState::PREPARE:
        {
            // keep prop_num_gen_ if first time try prepare;
            if (0ull != promised_num_) {
                prop_num_gen_.Next(promised_num_);
            }
            assert(prop_num_gen_.Get() > promised_num_);
            auto new_state = beginPreparePhase();
            assert(PropState::WAIT_PREPARE == new_state);

            auto tmp_rsp_msg_type = 
                updatePropState(PropState::WAIT_PREPARE);
            assert(MessageType::NOOP == tmp_rsp_msg_type);
            assert(PropState::WAIT_PREPARE == prop_state_);
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

            auto tmp_rsp_msg_type = 
                updatePropState(PropState::WAIT_ACCEPT);
            assert(MessageType::NOOP == tmp_rsp_msg_type);
            assert(PropState::WAIT_ACCEPT == prop_state_);
            rsp_msg_type = MessageType::ACCPT;
        }
        break;
    default:
        hassert(false, "invalid PropState %d", 
                static_cast<int>(next_prop_state));
        break;
    }
    return rsp_msg_type;
}

PropState PaxosInstanceImpl::stepTryPropose(uint64_t hint_proposed_num)
{
    prop_state_ = PropState::PREPARE; // force reset stat
    // don't touch proposing_value;

    prop_num_gen_.Update(hint_proposed_num);
    return PropState::PREPARE;
}

PropState PaxosInstanceImpl::stepBeginPropose(
        uint64_t hint_proposed_num, 
        const std::string& proposing_value)
{
    assert(PropState::NIL == prop_state_);
    assert(0ull == promised_num_);

    prop_state_ = PropState::PREPARE;
    proposing_value_ = proposing_value;
    prop_num_gen_.Update(hint_proposed_num);
    return PropState::PREPARE;
}

PropState PaxosInstanceImpl::beginPreparePhase()
{
    assert(PropState::PREPARE == prop_state_);
    bool reject = updatePromised(prop_num_gen_.Get());
    if (reject) {
        return PropState::PREPARE;
    }

    assert(false == reject);
    if (max_accepted_hint_num_ < accepted_num_) {
        max_accepted_hint_num_ = accepted_num_;
        if (!prop_num_gen_.IsLocalNum(accepted_num_)) {
            proposing_value_ = accepted_value_;
            clearStrictPropFlag();
        }
    }

    // ignore rsp_votes_[self_id_]
    rsp_votes_.clear();
    return PropState::WAIT_PREPARE;
}

PropState PaxosInstanceImpl::beginAcceptPhase()
{
    assert(PropState::ACCEPT == prop_state_);
    bool reject = updateAccepted(
            prop_num_gen_.Get(), proposing_value_, false);
    if (reject) {
        return PropState::PREPARE;
    }

    assert(false == reject);
    rsp_votes_.clear();
    return PropState::WAIT_ACCEPT;
}

PropState PaxosInstanceImpl::stepPrepareRsp(
        uint64_t prop_num, 
        uint64_t peer_id, 
        uint64_t peer_promised_num, 
        uint64_t peer_accepted_num, 
        const std::string& peer_accepted_value)
{
    assert(0 < peer_id);
    assert(0 < major_cnt_);
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
        if (!peer_accepted_value.empty()) {
            if (peer_accepted_num > max_accepted_hint_num_) {
                max_accepted_hint_num_ = peer_accepted_num;
                proposing_value_ = peer_accepted_value;
                clearStrictPropFlag();
            }
        }
    }

    int promise_cnt = 0;
    int reject_cnt = 0;
    tie(promise_cnt, reject_cnt) = countVotes(rsp_votes_);
    logdebug("promise_cnt %d reject_cnt %d", promise_cnt, reject_cnt);
    if (reject_cnt >= major_cnt_) {
        // rejected by majority
        return PropState::PREPARE;
        // vote_res.frist + 1 (including self-vote)
    } else if (promise_cnt + 1 >= major_cnt_) {
        return PropState::ACCEPT;
    }
    
    return PropState::WAIT_PREPARE;
}

PropState PaxosInstanceImpl::stepAcceptRsp(
        uint64_t prop_num, 
        uint64_t peer_id, 
        uint64_t peer_accepted_num, bool fast_accept_rsp)
{
    assert(0 < peer_id);
    assert(0 < major_cnt_);
    assert(PropState::WAIT_ACCEPT == prop_state_);
    uint64_t max_proposed_num = prop_num_gen_.Get();
    if (prop_num != max_proposed_num) {
        // ignore: prop num mis-match
        return PropState::WAIT_ACCEPT;
    }

    updateRspVotes(
            peer_id, 
            fast_accept_rsp ? 
                max_proposed_num == peer_accepted_num : 
                max_proposed_num >= peer_accepted_num, 
            rsp_votes_);

    int accept_cnt = 0;
    int reject_cnt = 0;
    tie(accept_cnt, reject_cnt) = countVotes(rsp_votes_);
    if (reject_cnt >= major_cnt_) {
        return PropState::PREPARE;
    } else if (accept_cnt+1 >= major_cnt_) {
        return PropState::CHOSEN;
    }

    return PropState::WAIT_ACCEPT;
}


bool PaxosInstanceImpl::updatePromised(uint64_t prop_num)
{
    active_proposer_time_ = chrono::system_clock::now();
    if (promised_num_ > prop_num) {
        // reject
        return true;
    }

    promised_num_ = prop_num;
    setPendingSeq();
    return false;
}

bool PaxosInstanceImpl::updateAccepted(
        uint64_t prop_num, 
        const std::string& prop_value, 
        bool is_fast_accept)
{
    active_proposer_time_ = chrono::system_clock::now();
    if (promised_num_ > prop_num) {
        // reject
        return true;
    }

    if (true == is_fast_accept) {
        if(0ull != accepted_num_) {
            // do fast accept only when 0ull == accepted_num_
            // => so only once!
            return false; 
        }
        assert(0ull == accepted_num_);
    }

    promised_num_ = prop_num;
    accepted_num_ = prop_num;
    accepted_value_ = prop_value;
    setPendingSeq();
    return false;
}

bool 
PaxosInstanceImpl::isTimeout(const std::chrono::milliseconds& timeout) const
{
    return active_proposer_time_ + 
        timeout < std::chrono::system_clock::now();
}


PaxosInstance::PaxosInstance(int major_cnt, uint64_t prop_num)
    : ins_impl_(major_cnt, prop_num)
{

}

PaxosInstance::PaxosInstance(
        int major_cnt, 
        uint64_t proposed_num, 
        uint64_t promised_num, 
        uint64_t accepted_num, 
        const std::string& accepted_value, 
        PropState prop_state, 
        uint32_t store_seq)
    : ins_impl_(
            major_cnt, proposed_num, 
            promised_num, accepted_num, 
            accepted_value, prop_state, store_seq)
{

}

PaxosInstance::~PaxosInstance() = default;

MessageType PaxosInstance::Step(const Message& msg)
{
    return ins_impl_.step(msg);
}


bool PaxosInstance::IsChosen() const
{
    return PropState::CHOSEN == ins_impl_.getPropState();
}

bool PaxosInstance::IsTimeout(const std::chrono::milliseconds& timeout) const
{
    return ins_impl_.isTimeout(timeout);
}

std::unique_ptr<paxos::HardState> 
PaxosInstance::GetPendingHardState(
        uint64_t logid, uint64_t paxos_index) const
{
    assert(0ull < index);
    if (0 == ins_impl_.getPendingSeq()) {
        return nullptr; // no pending
    }

    auto hs = make_unique<HardState>();
    assert(nullptr != hs);

    hs->set_logid(logid);
    hs->set_index(paxos_index);
    hs->set_proposed_num(GetProposeNum());
    hs->set_promised_num(GetPromisedNum());
    hs->set_accepted_num(GetAcceptedNum());
    if (0ull != GetAcceptedNum()) {
        hs->set_accepted_value(GetAcceptedValue());
    }

    hs->set_seq(GetPendingSeq());
    assert(0 < hs->seq());
    return hs;
}

} // namespace paxos


