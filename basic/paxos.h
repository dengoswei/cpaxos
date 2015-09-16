#pragma once

#include <tuple>
#include <string>
#include <set>
#include <map>
#include <memory>
#include <mutex>
#include <condition_variable>
#include <functional>
#include <stdint.h>



namespace paxos {


class PaxosInstance;

class Paxos {

public:

    typedef std::function<int(
            uint64_t, uint64_t,  
            const unique_ptr<proto::HardState>&, 
            const unique_ptr<Message>&)> StepCallback;

    // <retcode, proposing index>
    std::tuple<int, uint64_t> Propose(const std::string& proposing_value);

    int Step(uint64_t index, const Message& msg, StepCallback callback);

    uint64_t GetMaxIndex() { return max_index_; }
    uint64_t GetCommitedIndex() { return commited_index_; }

    int Wait(uint64_t index);


private:
    std::tuple<
        std::unique_ptr<proto::HardState>, 
        std::unique_ptr<Message>, 
        std::string> produceRsp(
                uint64_t index, 
                const PaxosInstance* ins, MessageType rsp_msg_type);


private:
    std::mutex paxos_mutex_;
    std::condition_variable paxos_cv_;

    uint64_t selfid_ = 0;
    std::set<uint64_t> peer_set_;

    uint64_t max_index_ = 0;
    uint64_t commited_index_ = 0;
    uint64_t next_commited_index_ = 0;
    std::set<uint64_t> chosen_set_;
    std::map<uint64_t, std::unique_ptr<PaxosInstance>> ins_map_;
};


} // namespace paxos




