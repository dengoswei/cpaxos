#include "paxos.h"
#include "paxos_impl.h"
#include "paxos_instance.h"
#include "utils.h"
#include "paxos.pb.h"

using namespace std;

namespace paxos {

Paxos::Paxos(uint64_t selfid, uint64_t group_size)
    : paxos_impl_(new PaxosImpl(selfid, group_size))
{

}

Paxos::~Paxos() = default;


std::tuple<int, uint64_t>
Paxos::Propose(const std::string& proposing_value, Callback callback)
{
    lock_guard<mutex> lock(paxos_mutex_);

    // limit only one active propose
    unique_ptr<proto::HardState> hs;
    unique_ptr<Message> rsp_msg;
    auto new_index = paxos_impl_->NextProposingIndex();
    if (0 == new_index) {
        return make_tuple(-1, 0);
    }

    // TODO
    // auto new_ins = buildPaxosInstance(peer_set_.size(), selfid_, 0);
    auto new_ins = paxos_impl_->BuildNewPaxosInstance();
    assert(nullptr != new_ins);
    int ret = new_ins->Propose(proposing_value);
    assert(0 == ret); // always ret 0 on new PaxosInstance 

    Message fake_msg;
    fake_msg.to_id = paxos_impl_->GetSelfId();
    tie(hs, rsp_msg) = paxos_impl_->ProduceRsp(
            new_index, new_ins.get(), fake_msg, MessageType::PROP);
    ret = callback(new_index, hs, rsp_msg);
    if (0 != ret) {
        paxos_impl_->DiscardProposingInstance(new_index, move(new_ins));
        assert(nullptr == new_ins);
        return make_tuple(-2, 0);
    }

    paxos_impl_->CommitProposingInstance(new_index, move(new_ins));
    assert(nullptr == new_ins);
    
    return make_tuple(0, new_index);
}

int 
Paxos::Step(uint64_t index, const Message& msg, Callback callback)
{
    bool update = false;
    {
        lock_guard<mutex> lock(paxos_mutex_);
        uint64_t prev_commit_index = paxos_impl_->GetCommitedIndex();
        if (paxos_impl_->IsChosen(index)) {
            return 1;
        }

        PaxosInstance* ins = paxos_impl_->GetInstance(index);
        if (nullptr == ins) {
            return -1;
        }

        unique_ptr<proto::HardState> hs;
        unique_ptr<Message> rsp_msg;
        auto rsp_msg_type = ins->Step(msg);
        tie(hs, rsp_msg) = 
            paxos_impl_->ProduceRsp(index, ins, msg, rsp_msg_type);
        int ret = callback(index, hs, rsp_msg);
        if (0 != ret) {
            return ret;
        }

        assert(0 == ret);
        paxos_impl_->CommitStep(index);
        if (prev_commit_index < paxos_impl_->GetCommitedIndex()) {
            update = true;
        }
        logdebug("commited_index_ %" PRIu64 "\n", 
               paxos_impl_->GetCommitedIndex());
    }

    if (update) {
        paxos_cv_.notify_all();
    }
    return 0;
}

void
Paxos::Wait(uint64_t index)
{
    unique_lock<mutex> lock(paxos_mutex_);
    if (index <= paxos_impl_->GetCommitedIndex()) {
        return ;
    }

    paxos_cv_.wait(lock, [&](){
        return index <= paxos_impl_->GetCommitedIndex();
    });
}

uint64_t Paxos::GetMaxIndex() 
{
    std::lock_guard<std::mutex> lock(paxos_mutex_);
    return paxos_impl_->GetMaxIndex();
}

uint64_t Paxos::GetCommitedIndex() 
{
    std::lock_guard<std::mutex> lock(paxos_mutex_);
    return paxos_impl_->GetCommitedIndex();
}

uint64_t Paxos::GetSelfId() 
{
    std::lock_guard<std::mutex> lock(paxos_mutex_);
    return paxos_impl_->GetSelfId();
}



} // namespace paxos


