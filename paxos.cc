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



Paxos::Paxos(uint64_t logid, 
        uint64_t selfid, uint64_t group_size, PaxosCallBack callback)
    : paxos_impl_{new PaxosImpl{logid, selfid, group_size}}
    , callback_(callback)
{
    assert(nullptr != callback_.read);
    assert(nullptr != callback_.write);
    assert(nullptr != callback_.send);
}


Paxos::~Paxos() = default;

// TODO: FIX ERROR CODE
std::tuple<paxos::ErrorCode, uint64_t>
Paxos::Propose(const uint64_t index, 
        gsl::cstring_view<> proposing_value, bool exclude)
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
                return std::make_tuple(ErrorCode::BUSY, 0ull);
            }
        }
        else {
            if (index <= paxos_impl_->GetCommitedIndex()
                    || index > paxos_impl_->GetMaxIndex() + 1) {
                return std::make_tuple(ErrorCode::INVALID_INDEX, 0ull);
            }

            proposing_index = index;
        }

        assert(0 == index || index == proposing_index);
        if (true == exclude) {
            auto ins = paxos_impl_->GetInstance(proposing_index, false);
            if (nullptr != ins) {
                return std::make_tuple(ErrorCode::BUSY, 0ull);
            }
         }

        msg.set_logid(paxos_impl_->GetLogId());
        msg.set_index(proposing_index);
        msg.set_to_id(paxos_impl_->GetSelfId());
    }

    assert(0 < msg.index());
    auto ret = Step(msg);
    if (ErrorCode::OK != ret) {
        logerr("Step ret %d", ret);
        return std::make_tuple(ret, 0ull);
    }

    return std::make_tuple(paxos::ErrorCode::OK, msg.index());
}

paxos::ErrorCode Paxos::Step(const Message& msg)
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
        // 1.
        std::lock_guard<std::mutex> lock(paxos_mutex_);
        prev_commit_index = paxos_impl_->GetCommitedIndex();

        PaxosInstance* ins = paxos_impl_->GetInstance(index, true);
        if (nullptr == ins) {
            assert(true == paxos_impl_->IsChosen(index));
             
            // construct a chosen paxos instance
            auto chosen_hs = callback_.read(index);
            if (nullptr == chosen_hs) {
                logerr("callback_.read index %" PRIu64 " nullptr", index);
                return paxos::ErrorCode::STORAGE_READ_ERROR;
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
            hs->set_logid(paxos_impl_->GetLogId());
        }
    }

    // 2.
    int ret = 0;
    if (nullptr != hs) {
        ret = callback_.write(*hs); 
        if (0 != ret) {
            logdebug("callback_.write index %" PRIu64 " ret %d", 
                    hs->index(), ret);
            return paxos::ErrorCode::STORAGE_WRITE_ERROR;
        }
    }

    assert(0 == ret);
    if (nullptr != rsp_msg) {
        ret = callback_.send(*rsp_msg);
        if (0 != ret) {
            logdebug("callback_.send index %" PRIu64 " msg type %d ret %d", 
                    rsp_msg->index(), static_cast<int>(rsp_msg->type()), ret);
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

    // TODO: after the lock_guard destructor
    if (update) {
        paxos_cv_.notify_all();
    }

    return paxos::ErrorCode::OK;
}

std::tuple<
paxos::ErrorCode, uint64_t, std::unique_ptr<HardState>> Paxos::Get(uint64_t index)
{
    assert(0 < index);
    uint64_t commited_index = 0;
    {
        std::lock_guard<std::mutex> lock(paxos_mutex_);
        commited_index = paxos_impl_->GetCommitedIndex();
        if (0 != commited_index && 
                index > paxos_impl_->GetMaxIndex()) {
            return make_tuple(ErrorCode::INVALID_INDEX, 0ull, nullptr);
        }

        if (index > commited_index) {
            return make_tuple(
                    ErrorCode::UNCOMMITED_INDEX, commited_index, nullptr);
        }
    }

    auto chosen_hs = callback_.read(index);
    if (nullptr == chosen_hs) {
        return make_tuple(ErrorCode::STORAGE_READ_ERROR, 0ull, nullptr);    
    }

    return make_tuple(ErrorCode::OK, commited_index, move(chosen_hs));
}

std::tuple<paxos::ErrorCode, uint64_t> 
Paxos::TrySet(uint64_t index, gsl::cstring_view<> proposing_value)
{
    // set exclude == true;
    return Propose(index, proposing_value, true);    
}

void Paxos::Wait(uint64_t index)
{
    unique_lock<mutex> lock(paxos_mutex_);
    if (index <= paxos_impl_->GetCommitedIndex()) {
        return ;
    }

    paxos_cv_.wait(lock, [&](){
        return index <= paxos_impl_->GetCommitedIndex();
    });
}

bool Paxos::WaitFor(uint64_t index, const std::chrono::milliseconds timeout)
{
    unique_lock<mutex> lock(paxos_mutex_);
    if (index <= paxos_impl_->GetCommitedIndex()) {
        return true;
    }

    auto time_point = chrono::system_clock::now() + timeout;
    return paxos_cv_.wait_until(lock, time_point, 
            [&]() -> bool {
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


