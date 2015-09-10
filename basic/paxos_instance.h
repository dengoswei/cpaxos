#pragma once

#include <stdint.h>
#include <string>
#include <map>
#include "utils.h"

// private
namespace paxos_impl {

enum class PropState : uint8_t {
    NIL = 0, 
    PREPARE, 
    WAIT_PREPARE, 
    ACCEPT, 
    WAIT_ACCEPT, 
    CHOSEN, 
};

class PaxosInstanceImpl {

public:
    PaxosInstanceImpl(int major_cnt, uint64_t prop_num);

    paxos::MessageType step(const paxos::Message& msg);

    paxos::MessageType updatePropState(PropState next_prop_state);

    // const function
    PropState getPropState() const { return prop_state_; }
    uint64_t getProposeNum() const { return prop_num_gen_.Get(); }
    uint64_t getPromisedNum() const { return promised_num_; }
    uint64_t getAcceptedNum() const { return accepted_num_; }
    const std::string& getAcceptedValue() const { return accepted_value_; }

    // proposer
    int beginPropose(const std::string& proposing_value);
    PropState beginPreparePhase();
    PropState beginAcceptPhase();

    PropState stepPrepareRsp(
            uint64_t prop_num, 
            uint64_t peer_id, uint64_t peer_promised_num, 
            uint64_t peer_accepted_num, 
            const std::string* peer_accepted_value);

    PropState stepAcceptRsp(
            uint64_t prop_num, 
            uint64_t peer_id, uint64_t peer_promised_num);

    // acceptor
    bool updatePromised(uint64_t prop_num);
    bool updateAccepted(uint64_t prop_num, const std::string& prop_value);

private:
    const int major_cnt_;
    paxos::PropNumGen prop_num_gen_;

    PropState prop_state_ = PropState::NIL; 

    uint64_t max_accepted_hint_num_ = 0;
    std::map<uint64_t, bool> rsp_votes_;

    // proposer
    std::string proposing_value_;

    // acceptor
    uint64_t promised_num_ = 0;
    uint64_t accepted_num_ = 0;
    std::string accepted_value_;
};

} // namespace paxos_impl

// public
namespace paxos {

class PaxosInstance {

public: 
    PaxosInstance(int group_size, uint64_t prop_num);

    int Propose(const std::string& proposing_value);

    MessageType Step(const Message& msg);

private:
    paxos_impl::PaxosInstanceImpl ins_impl_;
};

} // namespace paxos
