#include "paxos.h"
#include "paxos_impl.h"
#include "paxos_instance.h"
#include "utils.h"
#include "paxos.pb.h"

using namespace std;

namespace {

using namespace paxos;

std::unique_ptr<HardState> 
    CreateHardState(uint64_t index, const PaxosInstance* ins)
{
    assert(0 < index);
    assert(nullptr != ins);

    auto hs = unique_ptr<HardState>{new HardState{}};
    assert(nullptr != hs);

    hs->set_index(index);
    hs->set_proposed_num(ins->GetProposeNum());
    hs->set_promised_num(ins->GetPromisedNum());
    hs->set_accepted_num(ins->GetAcceptedNum());
    hs->set_accepted_value(ins->GetAcceptedValue());
    return hs;
}

}

namespace paxos {

Paxos::Paxos(uint64_t selfid, uint64_t group_size)
    : paxos_impl_(new PaxosImpl(selfid, group_size))
{

}

Paxos::~Paxos() = default;


std::tuple<int, uint64_t>
Paxos::Propose(gsl::cstring_view<> proposing_value, Callback callback)
{
    uint64_t new_index = 0;
    unique_ptr<PaxosInstance> new_ins;

    uint64_t store_seq = 0;
    unique_ptr<HardState> hs;
    unique_ptr<Message> rsp_msg;

    {
        lock_guard<mutex> lock(paxos_mutex_);

        // limit only one active propose
        new_index = paxos_impl_->NextProposingIndex();
        if (0 == new_index) {
            return make_tuple(-1, 0);
        }

        // TODO
        // auto new_ins = buildPaxosInstance(peer_set_.size(), selfid_, 0);
        new_ins = paxos_impl_->BuildNewPaxosInstance();
        assert(nullptr != new_ins);
        int ret = new_ins->Propose(proposing_value);
        assert(0 == ret); // always ret 0 on new PaxosInstance 

        Message fake_msg;
        fake_msg.set_to_id(paxos_impl_->GetSelfId());
        tie(store_seq, rsp_msg) = 
            paxos_impl_->ProduceRsp(
                    new_index, new_ins.get(), fake_msg, MessageType::PROP);
        if (0 != store_seq) {
            hs = CreateHardState(new_index, new_ins.get());
            assert(nullptr != hs);
            hs->set_index(new_index);
        }
    }

    int ret = callback(move(hs), move(rsp_msg));
    assert(nullptr == hs);
    assert(nullptr == rsp_msg);
    
    lock_guard<mutex> lock(paxos_mutex_);
    if (0 != ret) {
        logerr("Propose callback ret %d", ret);
        assert(nullptr != new_ins);
        paxos_impl_->DiscardProposingInstance(new_index, move(new_ins));
        assert(nullptr == new_ins);
        return make_tuple(-2, 0);
    }

    paxos_impl_->CommitProposingInstance(new_index, store_seq, move(new_ins));
    assert(nullptr == new_ins);
    return make_tuple(0, new_index);
}

int 
Paxos::Step(uint64_t index, const Message& msg, Callback callback)
{
    bool update = false;
    uint64_t prev_commit_index = 0;
    uint64_t store_seq = 0;
    unique_ptr<HardState> hs;
    unique_ptr<Message> rsp_msg;
    {
        lock_guard<mutex> lock(paxos_mutex_);
        prev_commit_index = paxos_impl_->GetCommitedIndex();
        if (paxos_impl_->IsChosen(index)) {
            return 1;
        }

        PaxosInstance* ins = paxos_impl_->GetInstance(index);
        if (nullptr == ins) {
            return -1;
        }

        auto rsp_msg_type = ins->Step(msg);
        tie(store_seq, rsp_msg) = 
            paxos_impl_->ProduceRsp(index, ins, msg, rsp_msg_type);
        if (0 != store_seq) {
            hs = CreateHardState(index, ins);
            assert(nullptr != hs);
            hs->set_index(index);
        }

        if (nullptr != rsp_msg) {
            rsp_msg->set_index(index);
        }
    }

    int ret = callback(move(hs), move(rsp_msg));
    assert(nullptr == hs);
    assert(nullptr == rsp_msg);
    {
        lock_guard<mutex> lock(paxos_mutex_);
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

std::tuple<uint64_t, uint64_t, uint64_t> Paxos::GetPaxosInfo()
{
    std::lock_guard<std::mutex> lock(paxos_mutex_);
    return make_tuple(
            paxos_impl_->GetSelfId(),
            paxos_impl_->GetMaxIndex(), 
            paxos_impl_->GetCommitedIndex());
}

// add for test
std::tuple<std::string, std::string> Paxos::GetInfo(uint64_t index)
{
    assert(0 < index);
    std::lock_guard<std::mutex> lock(paxos_mutex_);
    return paxos_impl_->GetInfo(index);
}

} // namespace paxos


