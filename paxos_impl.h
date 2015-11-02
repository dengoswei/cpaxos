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


namespace paxos {

class Message;
class PaxosInstance;

// NOT thread safe;
class PaxosImpl {

public:
    PaxosImpl(uint64_t selfid, uint64_t group_size);

    // NOTICE:
    // over-come std::unque_ptr uncomplete type;
    ~PaxosImpl();

    // help function
    uint64_t GetMaxIndex() { return max_index_; }
    uint64_t GetCommitedIndex() { return commited_index_; }
    uint64_t GetSelfId() { return selfid_; }


    uint64_t NextProposingIndex();
    std::unique_ptr<PaxosInstance> BuildNewPaxosInstance();
//    void DiscardProposingInstance(
//            uint64_t index, 
//            std::unique_ptr<PaxosInstance> proposing_ins);
//    void CommitProposingInstance(
//            uint64_t index, uint64_t store_seq, 
//            std::unique_ptr<PaxosInstance>&& proposing_ins);

    bool IsChosen(uint64_t index) {
        assert(0 < index);
        return index <= commited_index_;
    }

//    bool IsProposing(uint64_t index) {
//        assert(0 < index);
//        return index == proposing_index_;
//    }

    // may craete a new instance
    PaxosInstance* GetInstance(uint64_t index, bool create);
    void CommitStep(uint64_t index, uint64_t store_seq);

    // hs seq id & rsp msg
    std::tuple<
        uint64_t, std::unique_ptr<Message>>
    ProduceRsp(
            const PaxosInstance* ins, 
            const Message& req_msg, 
            MessageType rsp_msg_type);

    std::tuple<std::string, std::string> GetInfo(uint64_t index);

private:
//    Drop mutex protect
//    :=> paxos won't be thread safe;
    uint64_t selfid_ = 0;
    uint64_t group_size_ = 0;

    uint64_t max_index_ = 0;
    uint64_t commited_index_ = 0;
    uint64_t next_commited_index_ = 0;
    // uint64_t proposing_index_ = 0;

    uint64_t store_seq_ = 0;

    std::set<uint64_t> chosen_set_;
    std::map<uint64_t, uint64_t> pending_index_;
    std::map<uint64_t, std::unique_ptr<PaxosInstance>> ins_map_;
};


} // namespace paxos




