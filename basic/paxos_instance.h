#pragma once

#include <stdint.h>
#include <string>
#include <set>

namespace paxos {

class PaxosInstance {

public:
    int PaxosInstance::StepProposer();

private:
    enum {
        NIL, 
        PROP_PREPARE,
        PROP_WAIT_PREPARE_RSP, 
        PROP_ACCEPT, 
        PROP_WAIT_ACCEPT_RSP, 
    };

private:
    // proposer
    int beginPreparePhase(uint64_t next_proposed_num);
    int beginAcceptPhase();

    int stepPrepareRsp();

    // acceptor
    bool updatePromised(uint64_t prop_num);
    bool updateAccepted(uint64_t prop_num, const std::string& prop_value);

private:
    int prop_state_; 
    int accp_state_;

    bool chosen_;
    bool pending_store_;
    uint64_t index_;

    uint64_t max_accepted_hint_num_;
    std::string accepted_hint_value_;
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
