#pragma once

#include <memory>
#include <mutex>
#include <condition_variable>
#include <stdint.h>

namespace paxos {

class PaxosImpl;
struct Message;

namespace proto {

class HardState;

} // namespace proto

typedef std::function<int(
        uint64_t,
        const std::unique_ptr<proto::HardState>&, 
        const std::unique_ptr<Message>&)> Callback;


class Paxos {

public: 
    Paxos(uint64_t selfid, uint64_t group_size);

    // NOTICE:
    // over-come std::unque_ptr uncomplete type;
    ~Paxos();

    std::tuple<int, uint64_t>
        Propose(const std::string& proposing_value, Callback callback);

    int Step(uint64_t index, const Message& msg, Callback callback);

    void Wait(uint64_t index);

    uint64_t GetMaxIndex();
    uint64_t GetCommitedIndex();
    uint64_t GetSelfId();

private:
    std::mutex paxos_mutex_;
    std::condition_variable paxos_cv_;
    std::unique_ptr<PaxosImpl> paxos_impl_;
};


} // namespace paxos

