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


uint64_t
Paxos::Propose(gsl::cstring_view<> proposing_value, Callback callback)
{
    Message msg;
    msg.set_type(MessageType::BEGIN_PROP);
    msg.set_accepted_value(
            string{proposing_value.data(), proposing_value.size()});
    {
        lock_guard<mutex> lock(paxos_mutex_);
        msg.set_index(paxos_impl_->NextProposingIndex()); 
        msg.set_to_id(paxos_impl_->GetSelfId());
    }

    int ret = Step(msg, callback);
    if (0 != ret) {
        logerr("Step ret %d", ret);
    }

    return msg.index();
}

int Paxos::TryPropose(uint64_t index, Callback callback)
{
    assert(0 < index);
    Message msg;
    msg.set_type(MessageType::TRY_PROP);
    msg.set_index(index);
    {
        lock_guard<mutex> lock(paxos_mutex_);
        if (paxos_impl_->GetMaxIndex() < index) {
            logerr("selfid %" PRIu64 " max index %" PRIu64 " index %" PRIu64, 
                    paxos_impl_->GetSelfId(), paxos_impl_->GetMaxIndex(), index);
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

int 
Paxos::Step(const Message& msg, Callback callback)
{
    bool update = false;
    uint64_t prev_commit_index = 0;
    uint64_t store_seq = 0;
    unique_ptr<HardState> hs;
    unique_ptr<Message> rsp_msg;

    uint64_t index = msg.index();
    assert(0 < index);
    {
        lock_guard<mutex> lock(paxos_mutex_);
        prev_commit_index = paxos_impl_->GetCommitedIndex();

        PaxosInstance* ins = paxos_impl_->GetInstance(index, true);
        if (nullptr == ins) {
            if (paxos_impl_->IsChosen(index)) {
                return 1;
            }

            return -1;
        }

        auto rsp_msg_type = ins->Step(msg);
        tie(store_seq, rsp_msg) = 
            paxos_impl_->ProduceRsp(ins, msg, rsp_msg_type);
        if (0 != store_seq) {
            hs = CreateHardState(index, ins);
            assert(nullptr != hs);
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


