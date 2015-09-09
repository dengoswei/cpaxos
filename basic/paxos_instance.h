#pragma once

#include <stdint.h>
#include <string>
#include <map>
#include "utils.h"

namespace paxos {

class PaxosInstance {

public:
    PaxosInstance(int group_size, uint64_t prop_num);
    // TODO
    
    // normal propose
    int Propose(const std::string& proposing_value);
    
    // RETN:
    // true: indicate store state or send out msg;
    MessageType Step(const Message& msg);

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
    const int group_size_;
    PropNumGen prop_num_gen_;

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

} // namespace cpaxos
