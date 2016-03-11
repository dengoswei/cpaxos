#pragma once

#include <stdint.h>
#include <string>
#include <map>
#include <chrono>
#include "utils.h"
#include "paxos.pb.h"

// public
namespace paxos {

class Message;

// private
enum class PropState : uint8_t {
    NIL = 0, 
    PREPARE = 1, 
    WAIT_PREPARE = 2, 
    ACCEPT = 3, 
    WAIT_ACCEPT = 4, 
    CHOSEN = 5, 
};

class PaxosInstanceImpl {

public:
    PaxosInstanceImpl(int major_cnt, uint64_t prop_num);
    PaxosInstanceImpl(
            int major_cnt, 
            PropState prop_state, 
            const paxos::HardState& hs);

    MessageType step(const Message& msg);

    std::unique_ptr<Message>
        produceRsp(const Message& req_msg, MessageType rsp_msg_type);

    MessageType updatePropState(PropState next_prop_state);

    // const function
    PropState getPropState() const { return prop_state_; }
    uint64_t getProposeNum() const { return prop_num_gen_.Get(); }
    uint64_t getPromisedNum() const { return promised_num_; }
    uint64_t getAcceptedNum() const { return accepted_num_; }
    uint64_t getProposeEID() const { return proposing_value_.eid(); }

    const paxos::Entry& getAcceptedValue() const {
        return accepted_value_; 
    }

    // proposer
    PropState beginPreparePhase();
    PropState beginAcceptPhase();

    PropState stepTryPropose(uint64_t hint_proposed_num);

    PropState stepBeginPropose(
            uint64_t hint_proposed_num, 
            const paxos::Entry& proposing_value);

    PropState stepPrepareRsp(
            uint64_t prop_num, 
            uint64_t peer_id, uint64_t peer_promised_num, 
            uint64_t peer_accepted_num, 
            const paxos::Entry* peer_accepted_value);

    PropState stepAcceptRsp(
            uint64_t prop_num, 
            uint64_t peer_id, uint64_t peer_promised_num, 
            bool fast_accept_rsp);


    // acceptor
    bool updatePromised(uint64_t prop_num);
    bool updateAccepted(
            uint64_t prop_num, 
            const paxos::Entry& prop_value, 
            bool is_fast_accept);


    bool isTimeout(const std::chrono::milliseconds& timeout) const;

    uint32_t getPendingSeq() const {
        return pending_seq_;
    }

    void commitPendingSeq(const uint32_t seq) {
        if (seq == pending_seq_) {
            pending_seq_ = 0ull;
        }
    }

    void setPendingSeq() {
        assert(pending_seq_ <= store_seq_);
        pending_seq_ = ++store_seq_;
        assert(0 < pending_seq_);
    }

    bool getStrictPropFlag() const {
        return is_strict_prop_;
    }

    void setStrictPropFlag() {
        is_strict_prop_ = true;
    }

    void clearStrictPropFlag() {
        is_strict_prop_ = false;
    }

private:
    const int major_cnt_ = 0;
    paxos::PropNumGen prop_num_gen_;

    PropState prop_state_ = PropState::NIL; 

    uint64_t max_accepted_hint_num_ = 0ull;
    std::map<uint64_t, bool> rsp_votes_;

    // proposer
    bool is_strict_prop_ = false;
    paxos::Entry proposing_value_;

    // acceptor
    uint64_t promised_num_ = 0ull;
    uint64_t accepted_num_ = 0ull;
    paxos::Entry accepted_value_;

    std::chrono::time_point<
        std::chrono::system_clock> active_proposer_time_; 

    // pending_
    uint32_t store_seq_ = 0;
    uint32_t pending_seq_ = 0;
};


class PaxosInstance {

public: 
    PaxosInstance(int major_cnt, uint64_t prop_num);

    PaxosInstance(
            int major_cnt, 
            PropState prop_state, 
            const paxos::HardState& hs);

    // NOTICE:
    // over-come std::unque_ptr uncomplete type;
    ~PaxosInstance();

    MessageType Step(const Message& msg);

    std::unique_ptr<Message>
        ProduceRsp(const Message& req_msg, MessageType rsp_msg_type);

    PropState GetPropState() const { 
        return ins_impl_.getPropState();
    }

    uint64_t GetProposeEID() const { return ins_impl_.getProposeEID(); }
    uint64_t GetProposeNum() const { return ins_impl_.getProposeNum(); }
    uint64_t GetPromisedNum() const { return ins_impl_.getPromisedNum(); }
    uint64_t GetAcceptedNum() const { return ins_impl_.getAcceptedNum(); }
    const paxos::Entry& GetAcceptedValue() const {
        return ins_impl_.getAcceptedValue(); 
    }
    
    bool IsChosen() const;

    bool IsTimeout(const std::chrono::milliseconds& timeout) const;

    uint32_t GetPendingSeq() const {
        return ins_impl_.getPendingSeq();
    }

    void CommitPendingSeq(uint32_t seq) {
        ins_impl_.commitPendingSeq(seq);
    }

    std::unique_ptr<paxos::HardState> 
        GetPendingHardState(uint64_t logid, uint64_t paxos_index) const;

    bool GetStrictPropFlag() const {
        return ins_impl_.getStrictPropFlag();
    }

private:
    PaxosInstanceImpl ins_impl_;
};

} // namespace paxos
