#pragma once

#include <tuple>
#include <string>
#include <set>
#include <map>
#include <deque>
#include <memory>
#include <functional>
#include <cassert>
#include <stdint.h>
#include "paxos.pb.h"


namespace paxos {

class Message;
class PaxosInstance;

enum class PropState : uint8_t;

extern const size_t MAX_INS_SIZE;

// NOT thread safe;
class PaxosImpl {

public:
    PaxosImpl(
        uint64_t logid, 
        uint64_t selfid, 
        const std::set<uint64_t>& group_ids);

    PaxosImpl(
        uint64_t logid, 
        uint64_t selfid, 
        const std::set<uint64_t>& group_ids, 
        const std::deque<std::unique_ptr<paxos::HardState>>& hs_deque, 
        bool all_index_commited);

    // NOTICE:
    // over-come std::unque_ptr uncomplete type;
    ~PaxosImpl();

    // help function
    void UpdateMaxIndex(uint64_t new_max_index);
    uint64_t GetMaxIndex() const { return max_index_; }
    uint64_t GetCommitedIndex() const { return commited_index_; }
    uint64_t GetNextCommitedIndex() const { return next_commited_index_; }
    uint64_t GetSelfId() const { return selfid_; }
    uint64_t GetLogId() const { return logid_; }
    const std::set<uint64_t>& GetGroupIds() const {
        return group_ids_;
    }

    uint64_t NextProposingIndex();
    
    std::unique_ptr<PaxosInstance> 
        BuildPaxosInstance(const HardState& hs, PropState prop_state);

    bool IsChosen(uint64_t index) const {
        assert(0 < index);
        return index <= commited_index_;
    }

    bool UpdateNextCommitedIndex(uint64_t chosen_index);

    // may craete a new instance
    PaxosInstance* GetInstance(uint64_t index, bool create);
    void CommitStep(uint64_t index, uint32_t store_seq);

    void CommitStep(
            uint64_t index, 
            const std::vector<std::unique_ptr<paxos::HardState>>& vec_hs);

    bool CanFastProp(uint64_t prop_index);


private:
//    Drop mutex protect
//    :=> paxos won't be thread safe;
    const uint64_t logid_;
    const uint64_t selfid_;
    std::set<uint64_t> group_ids_;

    uint64_t max_index_ = 0;
    uint64_t commited_index_ = 0;
    uint64_t next_commited_index_ = 0;

    // std::set<uint64_t> chosen_set_;
    std::map<uint64_t, std::unique_ptr<PaxosInstance>> ins_map_;
};


MessageType Step(
        PaxosImpl& paxos_impl, 
        const Message& req_msg, 
        PaxosInstance* disk_ins);

std::vector<std::unique_ptr<Message>>
ProduceRsp(
        PaxosImpl& paxos_impl, 
        const Message& req_msg, 
        MessageType rsp_msg_type, 
        PaxosInstance* disk_ins);

} // namespace paxos




