#pragma once

#include <set>
#include <memory>
#include <mutex>
#include <condition_variable>
#include <functional>
#include <chrono>
#include <stdint.h>
#include "paxos_instance.h"
#include "paxos_impl.h"
#include "paxos.pb.h"

namespace paxos {

class PaxosImpl;
class Message;
class HardState;


struct PaxosCallBack {

    std::function<std::tuple<
        int, std::unique_ptr<HardState>>(uint64_t, uint64_t)> read;
    // std::function<std::unique_ptr<HardState>(uint64_t, uint64_t)> read;
    //
    std::function<int(const std::vector<std::unique_ptr<HardState>>&)> write;
    // std::function<int(std::unique_ptr<HardState>)> write;

    std::function<int(std::vector<std::unique_ptr<Message>>)> send;
    // std::function<int(std::unique_ptr<Message>)> send;
};


class Paxos {

public: 
    Paxos(
        uint64_t logid, uint64_t selfid, 
        const std::set<uint64_t>& group_ids, PaxosCallBack callback);

    Paxos(
        uint64_t selfid, 
        const SnapshotMetadata& meta, 
        PaxosCallBack callback);

    // NOTICE:
    // over-come std::unque_ptr uncomplete type;
    ~Paxos();

    // err, index
    std::tuple<paxos::ErrorCode, uint64_t>
        Propose(
                uint64_t index, 
                uint64_t reqid, 
                const std::string& proposing_value);

    paxos::ErrorCode Step(const Message& msg);

    paxos::ErrorCode 
        CheckAndFixTimeout(const std::chrono::milliseconds& timeout);

    void Wait(uint64_t index);
    bool WaitFor(
            uint64_t index, const std::chrono::milliseconds timeout);

    uint64_t GetMaxIndex();
    uint64_t GetCommitedIndex();
    uint64_t GetSelfId() const;
    uint64_t GetLogId() const;

    bool IsChosen(uint64_t index);

    // return 1: chosen && AcceptedValue().reqid() == reqid
    // return 0: chosen && AcceptedValue().reqid() != reqid
    // return <0: error case
    int CheckChosen(uint64_t index, uint64_t reqid);

    std::unique_ptr<SnapshotMetadata> CreateSnapshotMetadata();


private:
    std::tuple<
        std::vector<std::unique_ptr<paxos::HardState>>, 
        std::vector<std::unique_ptr<paxos::Message>>>
    CheckTimeoutNoLock(uint64_t exclude_index, std::chrono::milliseconds& timeout);

private:
    std::mutex prop_mutex_;
    std::mutex paxos_mutex_;
    std::condition_variable paxos_cv_;
    std::unique_ptr<PaxosImpl> paxos_impl_;

    PaxosCallBack callback_;
};


} // namespace paxos


