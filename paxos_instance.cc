#include "paxos_instance.h"

using namespace std;

#define PROP_TIMEOUT_TICK 100 // 100ms ?

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


PaxosInstanceImpl::PaxosInstanceImpl(int major_cnt, uint64_t prop_num)
    : major_cnt_(major_cnt)
    , prop_num_gen_(prop_num)
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
                    msg.proposed_num(), msg.peer_id(), 
                    msg.promised_num(), msg.accepted_num(), 
                    msg.accepted_value());
            rsp_msg_type = updatePropState(next_prop_state);
            logdebug("%s next_prop_state %d rsp_msg_type %d", 
                    __func__, static_cast<int>(next_prop_state), 
                    static_cast<int>(rsp_msg_type));
        }
        break;
    case MessageType::ACCPT_RSP:
        {
            if (PropState::WAIT_ACCEPT != prop_state_) {
                logdebug("msgtype::ACCPT_RSP but instance in stat %d", 
                        static_cast<int>(prop_state_));
                break;
            }

            assert(PropState::WAIT_ACCEPT == prop_state_);
            assert(true == msg.accepted_value().empty());
            auto next_prop_state = stepAcceptRsp(
                    msg.proposed_num(), msg.peer_id(), msg.accepted_num());
            rsp_msg_type = updatePropState(next_prop_state);
            logdebug("%s rsp_msg_type %d\n", 
                    __func__, static_cast<int>(rsp_msg_type));
        }
        break;
    case MessageType::PROP:
        updatePromised(msg.proposed_num());
        rsp_msg_type = MessageType::PROP_RSP;
        break;
    case MessageType::ACCPT:
        updateAccepted(msg.proposed_num(), msg.accepted_value());
        rsp_msg_type = MessageType::ACCPT_RSP;
        break;
    case MessageType::CHOSEN:
        {
            if (msg.accepted_num() == accepted_num_) {
                // mark as chosen
                rsp_msg_type = updatePropState(PropState::CHOSEN);
                break;
            }

            assert(false == msg.accepted_value().empty());

            // reset
            if (prop_num_gen_.Get() < promised_num_) {
                uint64_t prev_prop_num = prop_num_gen_.Get();
                prop_num_gen_.Next(promised_num_);
                assert(prev_prop_num < prop_num_gen_.Get());
            }

            // self promised
            assert(false == updatePromised(prop_num_gen_.Get()));
            // self accepted
            assert(false == updateAccepted(
                        prop_num_gen_.Get(), msg.accepted_value()));
            updatePropState(PropState::CHOSEN);
        }
        break;

    // propose
    case MessageType::BEGIN_PROP:
        {
            if (PropState::NIL != prop_state_) {
                logerr("msgtype::BEGIN_PROP but instance in stat %d", 
                        static_cast<int>(prop_state_));
                break;
            }

            assert(PropState::NIL == prop_state_);
            auto next_prop_state = stepBeginPropose(
                    false, msg.proposed_num(), msg.accepted_value());
            assert(PropState::PREPARE == next_prop_state);
            rsp_msg_type = updatePropState(next_prop_state);
            assert(PropState::WAIT_PREPARE == prop_state_);
        }
        break;
    case MessageType::TRY_REDO_PROP:
        {
            // TODO
            if (ACTIVE_TIME_OUT > 
                    chrono::duration_cast<chrono::milliseconds>(
                        chrono::system_clock::now() - active_proposer_time_)) {
                // no yet timeout => do nothing
                break;
            }

            // enable: try_prop_
            auto next_prop_state = stepBeginPropose(
                    true, msg.proposed_num(), msg.accepted_value());
            assert(PropState::PREPARE == next_prop_state);
            rsp_msg_type = updatePropState(next_prop_state);
            assert(PropState::WAIT_PREPARE == prop_state_);
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
            prop_num_gen_.Next(promised_num_);
            assert(prop_num_gen_.Get() >= promised_num_);
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

PropState PaxosInstanceImpl::stepBeginPropose(
        bool force, 
        uint64_t hint_proposed_num, 
        const std::string& proposing_value)
{
    assert(false == force || PropState::NIL == prop_state_);

    prop_state_ = PropState::PREPARE;
    proposing_value_ = proposing_value;

    promised_num_ = max(promised_num_, hint_proposed_num);
    return PropState::PREPARE;
}

//int PaxosInstanceImpl::beginPropose(
//        const gsl::cstring_view<>& proposing_value, 
//        bool force)
//{
//    if (PropState::NIL != prop_state_ 
//            || 0 != accepted_num_
//            || !force)  
//    {
//        // refuse to propose if
//        // - prop in this instance before;
//        // - instance have accepted other prop before
//        return -1;
//    }
//
//    assert(PropState::NIL == prop_state_ || true == force);
//    prop_state_ = PropState::PREPARE;
//    proposing_value_ = string{
//        proposing_value.data(), proposing_value.size()};
//
//    if (prop_num_gen_.Get() < promised_num_) {
//        uint64_t prev_prop_num = prop_num_gen_.Get();
//        prop_num_gen_.Next(promised_num_);
//        assert(prev_prop_num < prop_num_gen_.Get());
//    }
//    assert(prop_num_gen_.Get() >= promised_num_);
//    auto next_prop_state = beginPreparePhase();
//    hassert(PropState::WAIT_PREPARE == next_prop_state, 
//            "%" PRIu64 " %" PRIu64 " %d\n", 
//            prop_num_gen_.Get(), promised_num_, 
//            static_cast<int>(prop_state_));
//    updatePropState(next_prop_state);
//    assert(PropState::WAIT_PREPARE == prop_state_);
//    return 0;
//}

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
        proposing_value_ = accepted_value_;
    }

    // ignore rsp_votes_[self_id_]
    rsp_votes_.clear();
    return PropState::WAIT_PREPARE;
}

PropState PaxosInstanceImpl::beginAcceptPhase()
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
        uint64_t peer_accepted_num)
{
    assert(0 < peer_id);
    assert(0 < major_cnt_);
    assert(PropState::WAIT_ACCEPT == prop_state_);
    uint64_t max_proposed_num = prop_num_gen_.Get();
    if (prop_num != max_proposed_num) {
        // ignore: prop num mis-match
        return PropState::WAIT_ACCEPT;
    }

    updateRspVotes(peer_id, 
            max_proposed_num >= peer_accepted_num, rsp_votes_);

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
    active_proposer_tick_ = GetCurrentTick();
    if (promised_num_ > prop_num) {
        // reject
        return true;
    }

    promised_num_ = prop_num;
    return false;
}

bool PaxosInstanceImpl::updateAccepted(
        uint64_t prop_num, const std::string& prop_value)
{
    active_proposer_tick_ = GetCurrentTick();
    if (promised_num_ > prop_num) {
        // reject
        return true;
    }

    promised_num_ = prop_num;
    accepted_num_ = prop_num;
    accepted_value_ = prop_value;
    return false;
}




PaxosInstance::PaxosInstance(int major_cnt, uint64_t prop_num)
    : ins_impl_(major_cnt, prop_num)
{

}

PaxosInstance::~PaxosInstance() = default;


//int PaxosInstance::Propose(const gsl::cstring_view<>& proposing_value)
//{
//    return ins_impl_.beginPropose(proposing_value);
//}

MessageType PaxosInstance::Step(const Message& msg)
{
    return ins_impl_.step(msg);
}



} // namespace paxos


