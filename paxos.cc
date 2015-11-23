#include "paxos.h"
//#include "paxos_impl.h"
//#include "paxos_instance.h"
//#include "utils.h"
//#include "paxos.pb.h"

using namespace std;


namespace paxos {

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



Paxos::Paxos(uint64_t selfid, uint64_t group_size)
    : paxos_impl_(new PaxosImpl(selfid, group_size))
{

}

Paxos::Paxos(uint64_t selfid, uint64_t group_size, PaxosCallBack callback)
    : paxos_impl_{new PaxosImpl(selfid, group_size)}
    , callback_(callback)
{
    assert(nullptr != callback_.read);
    assert(nullptr != callback_.write);
    assert(nullptr != callback_.send);
}


Paxos::~Paxos() = default;

// TODO: FIX ERROR CODE
std::tuple<int, uint64_t>
Paxos::Propose(uint64_t index, gsl::cstring_view<> proposing_value)
{
    Message msg;
    msg.set_type(MessageType::BEGIN_PROP);
    msg.set_accepted_value(
            std::string{proposing_value.data(), proposing_value.size()});

    uint64_t proposing_index = 0;
    {
        std::lock_guard<std::mutex> lock(paxos_mutex_);
        if (0 == index) {
            // assign new index
            proposing_index = paxos_impl_->NextProposingIndex();        
            if (0 == proposing_index) {
                return std::make_tuple(-1, 0ull);
            }
        }
        else {
            if (index <= paxos_impl_->GetCommitedIndex()
                    || index > paxos_impl_->GetMaxIndex()) {
                return std::make_tuple(-2, 0ull);
            }
        }

        msg.set_index(proposing_index);
        msg.set_to_id(paxos_impl_->GetSelfId());
    }

    assert(0 < msg.index());
    int ret = Step(msg);
    if (0 != ret) {
        logerr("Step ret %d", ret);
        return std::make_tuple(-3, 0ull);
    }

    return std::make_tuple(0, msg.index());
}

int Paxos::Step(const Message& msg)
{
    bool update = false;
    uint64_t prev_commit_index = 0;
    uint64_t store_seq = 0;
    std::unique_ptr<HardState> hs;
    std::unique_ptr<Message> rsp_msg;

    // TODO: chosen_ins cache
    std::unique_ptr<PaxosInstance> chosen_ins;

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
            assert(true == paxos_impl_->IsChosen(index));
             
            // construct a chosen paxos instance
            auto chosen_hs = callback_.read(index);
            if (nullptr == chosen_hs) {
                logerr("callback_.read index %" PRIu64 " nullptr", index);
                return -1;
            }

            assert(index == chosen_hs->index());
            chosen_ins = paxos_impl_->BuildPaxosInstance(*chosen_hs, PropState::CHOSEN);
            assert(nullptr != chosen_ins);

            ins = chosen_ins.get();
            assert(static_cast<int>(PropState::CHOSEN) == ins->GetState());
        }

        auto rsp_msg_type = ins->Step(msg);
        std::tie(store_seq, rsp_msg) = 
            paxos_impl_->ProduceRsp(ins, msg, rsp_msg_type);
        if (0 != store_seq) {
            hs = CreateHardState(index, ins);
            assert(nullptr != hs);
        }
    }

    int ret = 0;
    if (nullptr != hs) {
        ret = callback_.write(*hs); 
        if (0 != ret) {
            return ret;
        }
    }

    assert(0 == ret);
    if (nullptr != rsp_msg) {
        ret = callback_.send(*rsp_msg);
        if (0 != ret) {
            logdebug("callback_.send ret %d", ret);
        }
    }

    {
        std::lock_guard<std::mutex> lock(paxos_mutex_);
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

std::tuple<int, std::unique_ptr<HardState>> Paxos::Get(uint64_t index)
{
    assert(0 < index);
    {
        std::lock_guard<std::mutex> lock(paxos_mutex_);
        if (index > paxos_impl_->GetMaxIndex()) {
            return make_tuple(-1, nullptr);
        }

        if (index > paxos_impl_->GetCommitedIndex()) {
            return make_tuple(1, nullptr);
        }
    }

    auto chosen_hs = callback_.read(index);
    if (nullptr == chosen_hs) {
        return make_tuple(-2, nullptr);    
    }

    return make_tuple(0, move(chosen_hs));
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


