#include "paxos_instance.h"

using namespace std;

namespace paxos {

int PaxosInstance::StepProposer()
{
    // TODO
}

int PaxosInstance::beginPreparePhase(uint64_t next_proposed_num)
{
    assert(PROP_PREPARE == prop_state_);
    assert(false == chosen_);
    assert(next_proposed_num >= max_proposed_num_);

    bool reject = updatePromised(next_proposed_num);
    if (reject) {
        // reject: error
        return -1;
    }

    assert(false == reject);
    if (max_accepted_hint_num_ < accepted_num_) {
        max_accepted_hint_num_ = accepted_num_;
        accepted_hint_value_ = accepted_value_;
    }

    // ignore rsp_votes_[self_id_]
    rsp_votes_.clear();
    max_proposed_num_ = next_proposed_num;
    pending_store_ = true;
    // TODO: 
    // 1. appendHardState
    // 2. appendMsg
    return 0;
}

int PaxosInstance::stepPrepareRsp(
        uint64_t prop_num, 
        uint64_t peer_id, 
        uint64_t peer_promised_num, 
        uint64_t* peer_accepted_num, 
        const std::string* peer_accepted_value)
{
    assert(PROP_WAIT_PREPARE_RSP == prop_state_);
    assert(0 < peer_id);
    if (prop_num != max_proposed_num_) {
        // ignore: prop num mis-match
        return -1;
    }

    rsp_votes_[peer_id] = max_proposed_num_ >= peer_promised_num;
    if (max_proposed_num_ >= peer_promised_num) {
        // peer promised
        if (nullptr != peer_accepted_num) {
            assert(nullptr != peer_accepted_value);
            if (*peer_accepted_num > max_accepted_hint_num_) {
                max_accepted_hint_num_ = *peer_accepted_num;
                accepted_hint_value_ = *peer_accepted_value;
            }
        }
    }

    // TODO:
    // major reject vs major promised
    
    return 0;
}

int PaxosInstance::stepAcceptRsp(
        uint64_t prop_num, 
        uint64_t peer_id, 
        uint64_t peer_promised_num)
{
    assert(PROP_WAIT_ACCEPT_RSP == prop_state_);
    assert(0 < peer_id);

    if (prop_num != max_proposed_num_) {
        // ignore: prop num mis-match
        return -1;
    }

    rsp_votes_[peer_id] = max_proposed_num_ >= peer_promised_num;
    // TODO:
    // major reject vs major accpted(chosen)
    return 0;
}

int PaxosInstance::beginAcceptPhase()
{
    assert(PROP_ACCEPT == prop_state_);
    assert(false == chosen_);

    // TODO: calculate proposing value: 
    //  {proposing_value_, accepted_hint_value_}
    const std::string& proposing_value = getProposingValue();
    bool reject = updateAccepted(
            max_proposed_num_, proposing_value);
    if (reject) {
        return -1;
    }

    assert(false == reject);

    rsp_votes_.clear();
    // TODO: appendMsg + appendHardState
    return 0;
}

bool PaxosInstance::updatePromised(uint64_t prop_num)
{
    if (promised_num_ > prop_num) {
        // reject
        return false;
    }

    promised_num_ = prop_num;
    pending_store_ = true;
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
    pending_store_ = true;
    return true;
}



} // namespace paxos


