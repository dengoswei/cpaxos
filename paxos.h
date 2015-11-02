#pragma once

#include <memory>
#include <mutex>
#include <condition_variable>
#include <stdint.h>
#include "gsl.h"

namespace paxos {

class PaxosImpl;
class Message;
class HardState;


typedef std::function<int(
        std::unique_ptr<HardState>, 
        std::unique_ptr<Message>)> Callback;


class Paxos {

public: 
    Paxos(uint64_t selfid, uint64_t group_size);

    // NOTICE:
    // over-come std::unque_ptr uncomplete type;
    ~Paxos();

    uint64_t Propose(gsl::cstring_view<> proposing_value, Callback callback);

    int TryPropose(uint64_t index, Callback callback);

    int Step(const Message& msg, Callback callback);

    void Wait(uint64_t index);

    uint64_t GetMaxIndex();
    uint64_t GetCommitedIndex();
    uint64_t GetSelfId();

    std::tuple<uint64_t, uint64_t, uint64_t> GetPaxosInfo();

    // add for test
    std::tuple<std::string, std::string> GetInfo(uint64_t index);

private:
    std::mutex paxos_mutex_;
    std::condition_variable paxos_cv_;
    std::unique_ptr<PaxosImpl> paxos_impl_;
};


} // namespace paxos

