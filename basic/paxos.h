#pragma once

#include <tuple>
#include <string>
#include <set>
#include <map>
#include <memory>
#include <mutex>
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

private:


private:
    std::mutex paxos_mutex_;


    uint64_t selfid_ = 0;
    std::set<uint64_t> peer_set_;

    uint64_t max_index_ = 0;
    uint64_t commited_index_ = 0;
    std::map<uint64_t, std::unique_ptr<PaxosInstance>> ins_map_;
};


} // namespace paxos




