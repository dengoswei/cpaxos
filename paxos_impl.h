#pragma once

#include <tuple>
#include <string>
#include <set>
#include <map>
#include <memory>
#include <functional>
#include <cassert>
#include <stdint.h>
#include "paxos.pb.h"
#include "utils.h"


namespace paxos {

class Message;
class PaxosInstance;

enum class PropState : uint8_t;

// NOT thread safe;
class PaxosImpl {

public:
    PaxosImpl(
        uint64_t logid, 
        uint64_t selfid, 
        const std::set<uint64_t>& group_ids);

    // NOTICE:
    // over-come std::unque_ptr uncomplete type;
    ~PaxosImpl();

    // help function
    uint64_t GetMaxIndex() const { return max_index_; }
    uint64_t GetCommitedIndex() const { return commited_index_; }
    uint64_t GetNextCommitedIndex() const { return next_commited_index_; }
    uint64_t GetSelfId() const { return selfid_; }
    uint64_t GetLogId() const { return logid_; }

    uint64_t NextProposingIndex();
    std::unique_ptr<PaxosInstance> BuildNewPaxosInstance();
    std::unique_ptr<PaxosInstance> 
        BuildPaxosInstance(const HardState& hs, PropState prop_state);

    bool IsChosen(uint64_t index) const {
        assert(0 < index);
        return index <= commited_index_;
    }

    void TryUpdateNextCommitedIndex();

    // may craete a new instance
    PaxosInstance* GetInstance(uint64_t index, bool create);
    void CommitStep(uint64_t index, uint64_t store_seq);

    // hs seq id, rsp msg
//    std::unique_ptr<Message>
    std::vector<std::unique_ptr<Message>>
    ProduceRsp(
            const PaxosInstance* ins, 
            const Message& req_msg, 
            MessageType rsp_msg_type);

    // msg, accepted_value
    std::tuple<std::string, std::string> GetInfo(uint64_t index) const;

    std::set<uint64_t> GetAllTimeoutIndex(
            const std::chrono::milliseconds timeout);

    bool UpdatePropNumGen(const uint64_t prop_num) {
        return prop_num_gen_.Update(prop_num);
    }

private:
//    Drop mutex protect
//    :=> paxos won't be thread safe;
    uint64_t logid_ = 0;
    uint64_t selfid_ = 0;
    std::set<uint64_t> group_ids_;

    uint64_t max_index_ = 0;
    uint64_t commited_index_ = 0;
    uint64_t next_commited_index_ = 0;
    paxos::PropNumGen prop_num_gen_;
    // uint64_t proposing_index_ = 0;

    uint64_t store_seq_ = 0;

    std::set<uint64_t> chosen_set_;
    std::map<uint64_t, std::unique_ptr<PaxosInstance>> ins_map_;
};


} // namespace paxos




