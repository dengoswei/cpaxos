#pragma once

#include <memory>
#include <mutex>
#include <condition_variable>
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


typedef std::function<int(
        std::unique_ptr<HardState>, 
        std::unique_ptr<Message>)> Callback;

std::unique_ptr<HardState>
    CreateHardState(uint64_t index, const PaxosInstance* ins);

class Paxos {

public: 
    Paxos(uint64_t selfid, uint64_t group_size);

    // NOTICE:
    // over-come std::unque_ptr uncomplete type;
    ~Paxos();

    template <typename CallBackType>
    std::tuple<int, uint64_t>
    Propose(gsl::cstring_view<> proposing_value, CallBackType callback);

    template <typename CallBackType>
    int TryPropose(uint64_t index, CallBackType callback);
    // int TryPropose(uint64_t index, Callback callback);

    template <typename CallBackType>
    int Step(const Message& msg, CallBackType callback);
    // int Step(const Message& msg, Callback callback);

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


template <typename CallBackType>
std::tuple<int, uint64_t>
Paxos::Propose(gsl::cstring_view<> proposing_value, CallBackType callback)
{
    Message msg;
    msg.set_type(MessageType::BEGIN_PROP);
    msg.set_accepted_value(
            std::string{proposing_value.data(), proposing_value.size()});
    {
        std::lock_guard<std::mutex> lock(paxos_mutex_);
        auto new_index = paxos_impl_->NextProposingIndex();
        if (0 == new_index) {
            return std::make_tuple(-1, 0ull);
        }

        msg.set_index(new_index);
        msg.set_to_id(paxos_impl_->GetSelfId());
    }

    assert(0 < msg.index());
    int ret = Step(msg, callback);
    if (0 != ret) {
        logerr("Step ret %d", ret);
        // TODO ?
    }

    return std::make_tuple(0, msg.index());
}


template <typename CallBackType>
int Paxos::TryPropose(uint64_t index, CallBackType callback)
{
    assert(0 < index);
    Message msg;
    msg.set_type(MessageType::TRY_PROP);
    msg.set_index(index);
    {
        std::lock_guard<std::mutex> lock(paxos_mutex_);
        if (paxos_impl_->GetMaxIndex() < index) {
            logerr("selfid %" PRIu64 " max index %" 
                    PRIu64 " index %" PRIu64, 
                    paxos_impl_->GetSelfId(), 
                    paxos_impl_->GetMaxIndex(), index);
            return -1;
        }

        msg.set_to_id(paxos_impl_->GetSelfId());
    }

    int ret = Step(msg, callback);
    if (0 != ret) {
        logerr("Step ret %d", ret);
    }

    return 0;
}

template <typename CallBackType>
int Paxos::Step(const Message& msg, CallBackType callback)
{
    bool update = false;
    uint64_t prev_commit_index = 0;
    uint64_t store_seq = 0;
    std::unique_ptr<HardState> hs;
    std::unique_ptr<Message> rsp_msg;

    uint64_t index = msg.index();
    hassert(0 < index, "index %" PRIu64 " type %d peer %" 
            PRIu64 " to %" PRIu64, 
            msg.index(), static_cast<int>(msg.type()), 
            msg.peer_id(), msg.to_id());
    assert(0 < index);
    {
        std::lock_guard<std::mutex> lock(paxos_mutex_);
        prev_commit_index = paxos_impl_->GetCommitedIndex();

        PaxosInstance* ins = paxos_impl_->GetInstance(index, true);
        if (nullptr == ins) {
            if (paxos_impl_->IsChosen(index)) {
                return 1;
            }

            return -1;
        }

        auto rsp_msg_type = ins->Step(msg);
        std::tie(store_seq, rsp_msg) = 
            paxos_impl_->ProduceRsp(ins, msg, rsp_msg_type);
        if (0 != store_seq) {
            hs = CreateHardState(index, ins);
            assert(nullptr != hs);
        }
    }

    int ret = callback(std::move(hs), std::move(rsp_msg));
    assert(nullptr == hs);
    assert(nullptr == rsp_msg);
    {
        std::lock_guard<std::mutex> lock(paxos_mutex_);
        if (0 != ret) {
            return ret;
        }

        assert(0 == ret);
        paxos_impl_->CommitStep(index, store_seq);
        if (prev_commit_index < paxos_impl_->GetCommitedIndex()) {
            update = true;
        }
        logdebug("selfid %" PRIu64 " commited_index_ %" PRIu64 "\n", 
               paxos_impl_->GetSelfId(), paxos_impl_->GetCommitedIndex());
    }

    if (update) {
        paxos_cv_.notify_all();
    }
    return 0;
}


} // namespace paxos


