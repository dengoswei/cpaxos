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



Paxos::Paxos(
        uint64_t logid, 
        uint64_t selfid, 
        const std::set<uint64_t>& group_ids, PaxosCallBack callback)
    : paxos_impl_(make_unique<PaxosImpl>(logid, selfid, group_ids))
    , callback_(callback)
{
    assert(nullptr != callback_.read);
    assert(nullptr != callback_.write);
    assert(nullptr != callback_.send);
}


Paxos::~Paxos() = default;

std::tuple<paxos::ErrorCode, uint64_t>
Paxos::Propose(const uint64_t index, const std::string& proposing_value)
{
    Message prop_msg;
    prop_msg.set_type(MessageType::BEGIN_PROP);
    prop_msg.set_accepted_value(proposing_value);

    std::lock_guard<std::mutex> prop_lock(prop_mutex_);
    {
        std::lock_guard<std::mutex> lock(paxos_mutex_);
        uint64_t prop_index = paxos_impl_->NextProposingIndex();
        if (0ull == prop_index || 
                (0ull != index && index != prop_index)) {
            return std::make_tuple(ErrorCode::BUSY, 0ull);
        }

        assert(0ull < prop_index);
        if (paxos_impl_->CanFastProp(prop_index)) {
            // try fast prop
            prop_msg.set_type(MessageType::BEGIN_FAST_PROP);
        }

        prop_msg.set_logid(paxos_impl_->GetLogId());
        prop_msg.set_to(paxos_impl_->GetSelfId());
        prop_msg.set_index(prop_index);
        prop_msg.set_proposed_num(paxos_impl_->GetProposeNum());
    }

    assert(0ull < prop_msg.index());
    auto ret = Step(prop_msg);
    if (ErrorCode::OK != ret) {
        logerr("Step selfid %" PRIu64 " index %" PRIu64 " ret %d", 
                prop_msg.to(), prop_msg.index(), ret);
        return std::make_tuple(ret, 0ull);
    }

    return std::make_tuple(paxos::ErrorCode::OK, prop_msg.index());
}

paxos::ErrorCode Paxos::Step(const Message& msg)
{
    uint64_t prev_commit_index = 0;
    std::unique_ptr<HardState> hs;
    std::vector<std::unique_ptr<Message>> vec_msg;

    uint64_t index = msg.index();
    hassert(0 < index, "index %" PRIu64 " type %d peer %" 
            PRIu64 " to %" PRIu64, 
            msg.index(), static_cast<int>(msg.type()), 
            msg.from(), msg.to());
    assert(0 < index);

    {
        // 1.
        unique_lock<mutex> lock(paxos_mutex_);
        assert(msg.logid() == paxos_impl_->GetLogId());
        prev_commit_index = paxos_impl_->GetCommitedIndex();

        unique_ptr<PaxosInstance> disk_ins = nullptr;
        if (index < paxos_impl_->GetCommitedIndex()) {
            auto ins = paxos_impl_->GetInstance(index, false);
            if (nullptr == ins) {
                // re-construct disk_ins
                auto group_size = paxos_impl_->GetGroupIds().size();
                lock.unlock();
                auto chosen_hs = callback_.read(msg.logid(), index);
                if (nullptr == chosen_hs) {
                    logerr("read index %" PRIu64 " nullptr", index);
                    return paxos::ErrorCode::STORAGE_READ_ERROR;
                }

                disk_ins = make_unique<PaxosInstance>(
                        group_size / 2 + 1, 
                        chosen_hs->proposed_num(), 
                        chosen_hs->promised_num(), 
                        chosen_hs->accepted_num(), 
                        chosen_hs->accepted_value(), 
                        PropState::CHOSEN, 
                        chosen_hs->seq());
                assert(nullptr != disk_ins);
                lock.lock();
            }
        }

        auto rsp_msg_type = 
            ::paxos::Step(*paxos_impl_, msg, disk_ins.get());
        vec_msg = ProduceRsp(
                *paxos_impl_, msg, rsp_msg_type, disk_ins.get());
        if (nullptr == disk_ins.get()) {
            auto ins = paxos_impl_->GetInstance(index, false);
            assert(nullptr != ins);
            hs = ins->GetPendingHardState(paxos_impl_->GetLogId(), index);
        }
    }

    // 2.
    int ret = 0;
    uint64_t store_seq = nullptr == hs ? 0ull : hs->seq();
    if (nullptr != hs) {
        ret = callback_.write(move(hs)); 
        if (0 != ret) {
            logdebug("callback_.write index %" PRIu64 " ret %d", 
                    hs->index(), ret);
            return paxos::ErrorCode::STORAGE_WRITE_ERROR;
        }
    }

    assert(0 == ret);
    if (false == vec_msg.empty()) {
        for (auto& rsp_msg : vec_msg) {
            assert(nullptr != rsp_msg);
            assert(0ull != rsp_msg->to());

            auto rsp_msg_type = rsp_msg->type();
            ret = callback_.send(move(rsp_msg));
            if (0 != ret) {
                logdebug("callback_.send index %" PRIu64 
                        " msg type %d ret %d", 
                        msg.index(), 
                        static_cast<int>(rsp_msg_type), ret);
            }
            assert(nullptr == rsp_msg);
        }

        vec_msg.clear();
    }

    bool update = false;
    {
        std::lock_guard<std::mutex> lock(paxos_mutex_);
        paxos_impl_->CommitStep(index, store_seq);
        if (prev_commit_index < paxos_impl_->GetCommitedIndex()) {
            update = true;
        }
        logdebug("selfid %" PRIu64 " commited_index_ %" PRIu64 "\n", 
               paxos_impl_->GetSelfId(), 
               paxos_impl_->GetCommitedIndex());
    }

    // TODO: after the lock_guard destructor
    if (update) {
        paxos_cv_.notify_all();
    }

    return paxos::ErrorCode::OK;
}

paxos::ErrorCode Paxos::CheckAndFixTimeout(
        const std::chrono::milliseconds& timeout)
{
    Message check_msg;
    check_msg.set_type(MessageType::TRY_PROP);

    std::lock_guard<std::mutex> prop_lock(prop_mutex_);
    {
        std::lock_guard<std::mutex> lock(paxos_mutex_);
        auto prop_index = paxos_impl_->NextProposingIndex();
        if (0ull != prop_index) {
            return ErrorCode::OK; // do nothing
        }

        assert(0ull == prop_index);
        auto check_index = paxos_impl_->GetMaxIndex();
        auto ins = paxos_impl_->GetInstance(check_index, false);
        assert(nullptr != ins);
        if (!ins->IsTimeout(timeout)) {
            return ErrorCode::OK; // not yet timeout
        }

        // timeout
        check_msg.set_logid(paxos_impl_->GetLogId());
        check_msg.set_to(paxos_impl_->GetSelfId());
        check_msg.set_index(prop_index);
        check_msg.set_proposed_num(paxos_impl_->GetProposeNum());
    }

    assert(0ull < check_msg.index());
    auto ret = Step(check_msg);
    if (ErrorCode::OK != ret) {
        logerr("Step selfid %" PRIu64 " index %" PRIu64 " ret %d", 
                check_msg.to(), check_msg.index(), ret);
    }

    return ret;
}

std::tuple<
paxos::ErrorCode, uint64_t, std::unique_ptr<HardState>> Paxos::Get(uint64_t index)
{
    assert(0 < index);
    uint64_t logid = 0ull;
    uint64_t commited_index = 0ull;
    {
        std::lock_guard<std::mutex> lock(paxos_mutex_);
        commited_index = paxos_impl_->GetCommitedIndex();
        if (0ull != commited_index && 
                index > paxos_impl_->GetMaxIndex()) {
            return make_tuple(ErrorCode::INVALID_INDEX, 0ull, nullptr);
        }

        if (index > commited_index) {
            return make_tuple(
                    ErrorCode::UNCOMMITED_INDEX, commited_index, nullptr);
        }

        logid = paxos_impl_->GetLogId();
    }

    auto chosen_hs = callback_.read(logid, index);
    if (nullptr == chosen_hs) {
        return make_tuple(ErrorCode::STORAGE_READ_ERROR, 0ull, nullptr);    
    }

    return make_tuple(ErrorCode::OK, commited_index, move(chosen_hs));
}

std::tuple<paxos::ErrorCode, uint64_t> 
Paxos::TrySet(uint64_t index, const std::string& proposing_value)
{
    return Propose(index, proposing_value);
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

std::set<uint64_t> 
Paxos::GetAllTimeoutIndex(const std::chrono::milliseconds timeout) 
{
    std::lock_guard<std::mutex> lock(paxos_mutex_);
    return paxos_impl_->GetAllTimeoutIndex(timeout);
}

} // namespace paxos


