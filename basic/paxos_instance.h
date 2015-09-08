#pragma once

#include <stdint.h>
#include <string>
#include <map>
#include "utils.h"

namespace paxos {

class PaxosInstance {

public:
    // TODO
    // RETN:
    // true: indicate store state or send out msg;
    MessageType StepProposer(const Message& msg);

    MessageType StepAcceptor(const Message& msg);

private:
    enum class PropState : uint8_t {
        NIL = 0, 
        PREPARE, 
        WAIT_PREPARE, 
        ACCEPT, 
        WAIT_ACCEPT, 
        CHOSEN, 
    };

private:
    MessageType updatePropState(PropState next_prop_state);

    // proposer
    PropState beginPreparePhase(uint64_t next_proposed_num);
    PropState beginAcceptPhase();

    PropState stepPrepareRsp(
            uint64_t prop_num, 
            uint64_t peer_id, uint64_t peer_promised_num, 
            uint64_t* peer_accepted_num, 
            const std::string* peer_accepted_value);

    PropState stepAcceptRsp(
            uint64_t prop_num, 
            uint64_t peer_id, uint64_t peer_promised_num);


    // acceptor
    bool updatePromised(uint64_t prop_num);
    bool updateAccepted(uint64_t prop_num, const std::string& prop_value);

private:
    const int major_cnt_;
    PropNumGen prop_num_gen_;

    PropState prop_state_; 

    bool chosen_;
    uint64_t index_;

    uint64_t max_accepted_hint_num_;
    std::map<uint64_t, bool> rsp_votes_;

    // proposer
    uint64_t max_proposed_num_;
    std::string proposing_value_;

    // acceptor
    uint64_t promised_num_;
    uint64_t accepted_num_;
    std::string accepted_value_;
};

} // namespace cpaxos
