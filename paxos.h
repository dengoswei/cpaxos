#pragma once

#include <memory>
#include <mutex>
#include <condition_variable>
#include <functional>
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

    std::function<std::unique_ptr<HardState>(uint64_t index)> read;
    std::function<int(const HardState&)> write;

    std::function<int(const Message&)> send;
};


class Paxos {

public: 
    Paxos(uint64_t selfid, uint64_t group_size);
    Paxos(uint64_t selfid, uint64_t group_size, PaxosCallBack callback);

    // NOTICE:
    // over-come std::unque_ptr uncomplete type;
    ~Paxos();

    std::tuple<int, uint64_t>
        Propose(uint64_t index, 
                gsl::cstring_view<> proposing_value, bool exclude);

    int Step(const Message& msg);

    std::tuple<int, uint64_t, std::unique_ptr<HardState>> Get(uint64_t index);
    std::tuple<int, uint64_t> TrySet(
            uint64_t index, gsl::cstring_view<> proposing_value);

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

    PaxosCallBack callback_;
};


} // namespace paxos


