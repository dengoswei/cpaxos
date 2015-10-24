#pragma once

#include <tuple>
#include <string>
#include <set>
#include <map>
#include <memory>
#include <functional>
#include <cassert>
#include <stdint.h>


namespace paxos {

namespace proto {

class HardState;

} // namespace proto

struct Message;
enum class MessageType : uint32_t;
class PaxosInstance;

typedef std::function<int(
        uint64_t,
        const std::unique_ptr<proto::HardState>&, 
        const std::unique_ptr<Message>&)> Callback;

// NOT thread safe;
class PaxosImpl {

public:
    PaxosImpl(uint64_t selfid, uint64_t group_size);

    // NOTICE:
    // over-come std::unque_ptr uncomplete type;
    ~PaxosImpl();

    // <retcode, proposing index>
    // IMPORTANT:
    // you can't call both Propose & Step the same time;
    // std::tuple<int, uint64_t> Propose(
    //         const std::string& proposing_value, Callback callback);
    // int Step(uint64_t index, const Message& msg, Callback callback);

    // help function
    uint64_t GetMaxIndex() { return max_index_; }
    uint64_t GetCommitedIndex() { return commited_index_; }
    uint64_t GetSelfId() { return selfid_; }


    uint64_t NextProposingIndex();
    std::unique_ptr<PaxosInstance> BuildNewPaxosInstance();
    void DiscardProposingInstance(
            uint64_t index, 
            std::unique_ptr<PaxosInstance>&& proposing_ins);
    void CommitProposingInstance(
            uint64_t index, 
            std::unique_ptr<PaxosInstance>&& proposing_ins);

    bool IsChosen(uint64_t index) {
        assert(0 < index);
        return index <= commited_index_;
    }

    bool IsProposing(uint64_t index) {
        assert(0 < index);
        return index == proposing_index_;
    }

    // may craete a new instance
    PaxosInstance* GetInstance(uint64_t index);
    void CommitStep(uint64_t index);

    std::tuple<
        std::unique_ptr<proto::HardState>, 
        std::unique_ptr<Message>>
    ProduceRsp(uint64_t index, 
            const PaxosInstance* ins, 
            const Message& req_msg, 
            MessageType rsp_msg_type);

private:
//    Drop mutex protect
//    :=> paxos won't be thread safe;
    uint64_t selfid_ = 0;
    uint64_t group_size_ = 0;

    uint64_t max_index_ = 0;
    uint64_t commited_index_ = 0;
    uint64_t next_commited_index_ = 0;
    uint64_t proposing_index_ = 0;

    std::set<uint64_t> chosen_set_;
    std::set<uint64_t> pending_index_;
    std::map<uint64_t, std::unique_ptr<PaxosInstance>> ins_map_;
};


} // namespace paxos




