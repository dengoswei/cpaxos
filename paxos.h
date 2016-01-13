#pragma once

#include <set>
#include <memory>
#include <mutex>
#include <condition_variable>
#include <functional>
#include <chrono>
#include <stdint.h>
#include "gsl.h"
#include "utils.h"
#include "paxos_instance.h"
#include "paxos_impl.h"
#include "paxos.pb.h"

namespace paxos {

class PaxosImpl;
class Message;
class HardState;


std::unique_ptr<HardState>
    CreateHardState(uint64_t index, const PaxosInstance* ins);

struct PaxosCallBack {

    std::function<std::unique_ptr<HardState>(uint64_t, uint64_t)> read;
    std::function<int(std::unique_ptr<HardState>)> write;

    std::function<int(std::unique_ptr<Message>)> send;
};


class Paxos {

public: 
    Paxos(
        uint64_t logid, uint64_t selfid, 
        const std::set<uint64_t>& group_ids, PaxosCallBack callback);

    // NOTICE:
    // over-come std::unque_ptr uncomplete type;
    ~Paxos();

    std::tuple<paxos::ErrorCode, uint64_t>
        Propose(uint64_t index, gsl::cstring_view<> proposing_value);

    paxos::ErrorCode Step(const Message& msg);

    paxos::ErrorCode CheckAndFixTimeout(const std::chrono::milliseconds& timeout);

    std::tuple<paxos::ErrorCode, uint64_t, std::unique_ptr<HardState>> Get(uint64_t index);
    std::tuple<paxos::ErrorCode, uint64_t> TrySet(
            uint64_t index, gsl::cstring_view<> proposing_value);

    void Wait(uint64_t index);
    bool WaitFor(uint64_t index, const std::chrono::milliseconds timeout);

    // get timeout paxos instance
    std::set<uint64_t> GetAllTimeoutIndex(
            const std::chrono::milliseconds timeout);

    uint64_t GetMaxIndex();
    uint64_t GetCommitedIndex();
    uint64_t GetSelfId();

    std::tuple<uint64_t, uint64_t, uint64_t> GetPaxosInfo();

    // add for test
    std::tuple<std::string, std::string> GetInfo(uint64_t index);

private:
    std::mutex prop_mutex_;
    std::mutex paxos_mutex_;
    std::condition_variable paxos_cv_;
    std::unique_ptr<PaxosImpl> paxos_impl_;

    PaxosCallBack callback_;
};


} // namespace paxos


