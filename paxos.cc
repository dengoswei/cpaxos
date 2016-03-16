#include <deque>
#include "paxos.h"
#include "mem_utils.h"
#include "log.h"
#include "hassert.h"

using namespace std;

namespace paxos {


Paxos::Paxos(
        uint64_t logid, 
        uint64_t selfid, 
        const std::set<uint64_t>& group_ids, PaxosCallBack callback)
    : paxos_impl_(cutils::make_unique<PaxosImpl>(logid, selfid, group_ids))
    , callback_(callback)
{
    assert(nullptr != callback_.read);
    assert(nullptr != callback_.write);
    assert(nullptr != callback_.send);
}

Paxos::Paxos(
        uint64_t selfid, 
        const paxos::SnapshotMetadata& meta, 
        PaxosCallBack callback)
    : paxos_impl_(nullptr)
    , callback_(callback)
{
    assert(nullptr != callback_.read);
    assert(nullptr != callback_.write);
    assert(nullptr != callback_.send);

    std::set<uint64_t> group_ids;
    {
        assert(true == meta.has_conf_state());
        auto& conf_state = meta.conf_state();
        assert(3 <= conf_state.nodes_size());
        for (int idx = 0; idx < conf_state.nodes_size(); ++idx) {
            group_ids.insert(conf_state.nodes(idx));
        }
    }

    assert(true == meta.has_logid());
    if (false == meta.has_max_index()) {
        // new plog
        paxos_impl_ = cutils::make_unique<
            PaxosImpl>(meta.logid(), selfid, group_ids);
        assert(nullptr != paxos_impl_);
        assert(0 == paxos_impl_->GetMaxIndex());
        assert(0 == paxos_impl_->GetCommitedIndex());
    }
    else {
        assert(true == meta.has_max_index());
        assert(true == meta.has_commited_index());
        assert(meta.commited_index() <= meta.max_index());
        int err = 0;
        std::unique_ptr<paxos::HardState> hs = nullptr;
        uint64_t index = meta.max_index();

        std::deque<std::unique_ptr<paxos::HardState>> hs_deque;
        for (; true; ++index) {
            assert(0 == err);
            assert(nullptr == hs);
            tie(err, hs) = callback_.read(meta.logid(), index);
            if (0 != err) {
                assert(nullptr == hs);
                assert(1 == err);
                break;
            }
            assert(0 == err);
            assert(nullptr != hs);
            assert(hs->index() == index);
            assert(hs->logid() == meta.logid());
            hs_deque.push_back(move(hs));
            assert(nullptr == hs);
            if (MAX_INS_SIZE < hs_deque.size()) {
                hs_deque.pop_front();
            }
        }

        assert(0 <= index);
        index = 0 == index ? index : index - 1;
        assert(index >= meta.max_index());
        // TODO
        paxos_impl_ = cutils::make_unique<PaxosImpl>(
                meta.logid(), selfid, group_ids, 
                hs_deque, meta.commited_index() == index);
        assert(nullptr != paxos_impl_);
        assert(index == paxos_impl_->GetMaxIndex());
        assert(index >= paxos_impl_->GetCommitedIndex());
    }

    assert(nullptr != paxos_impl_);
}


Paxos::~Paxos() = default;

std::tuple<paxos::ErrorCode, uint64_t, uint64_t>
Paxos::Propose(const uint64_t index, const std::string& proposing_value)
{
    Message prop_msg;
    prop_msg.set_type(MessageType::BEGIN_PROP);

    {
        auto prop_entry = prop_msg.mutable_accepted_value();
        assert(nullptr != prop_entry);
        prop_entry->set_type(EntryType::EntryNormal);
        prop_entry->set_data(proposing_value);
    }

    std::lock_guard<std::mutex> prop_lock(prop_mutex_);
    {
        std::lock_guard<std::mutex> lock(paxos_mutex_);
        uint64_t prop_index = paxos_impl_->NextProposingIndex();
        if (0ull == prop_index || 
                (0ull != index && index != prop_index)) {
            return std::make_tuple(ErrorCode::BUSY, 0ull, 0ull);
        }

        assert(0ull < prop_index);
        if (paxos_impl_->CanFastProp(prop_index)) {
            // try fast prop
            prop_msg.set_type(MessageType::BEGIN_FAST_PROP);
        }

        prop_msg.set_logid(paxos_impl_->GetLogId());
        prop_msg.set_to(paxos_impl_->GetSelfId());
        prop_msg.set_index(prop_index);
        // prop_msg.set_proposed_num(paxos_impl_->GetProposeNum());
    }

    assert(0ull < prop_msg.index());
    auto ret = Step(prop_msg);
    if (ErrorCode::OK != ret) {
        logerr("Step selfid %" PRIu64 " index %" PRIu64 " ret %d", 
                prop_msg.to(), prop_msg.index(), ret);
        return std::make_tuple(ret, 0ull, 0ull);
    }

    {
        std::lock_guard<std::mutex> lock(paxos_mutex_);
        auto ins = paxos_impl_->GetInstance(prop_msg.index(), false);
        assert(nullptr != ins);
        auto eid = ins->GetProposeEID();
        assert(0 < eid);
        return std::make_tuple(paxos::ErrorCode::OK, prop_msg.index(), eid);
    }
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
                int err = 0;
                std::unique_ptr<HardState> chosen_hs = nullptr;
                tie(err, chosen_hs) = callback_.read(msg.logid(), index);
                if (0 != err) {
                    assert(nullptr == chosen_hs);
                    logerr("read index %" PRIu64 " err %d", index, err);
                    return paxos::ErrorCode::STORAGE_READ_ERROR;
                }

                assert(0 == err);
                assert(nullptr != chosen_hs);
                disk_ins = cutils::make_unique<PaxosInstance>(
                        group_size / 2 + 1, PropState::CHOSEN, *chosen_hs);
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
                    msg.index(), ret);
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
            return ErrorCode::NO_TIMEOUT_INDEX; // do nothing
        }

        assert(0ull == prop_index);
        auto check_index = paxos_impl_->GetMaxIndex();
        auto ins = paxos_impl_->GetInstance(check_index, false);
        assert(nullptr != ins);
        if (!ins->IsTimeout(timeout)) {
            return ErrorCode::NO_TIMEOUT_INDEX; // not yet timeout
        }

        // timeout
        prop_index = check_index;
        check_msg.set_logid(paxos_impl_->GetLogId());
        check_msg.set_to(paxos_impl_->GetSelfId());
        check_msg.set_index(prop_index);
        check_msg.set_proposed_num(ins->GetProposeNum());
    }

    assert(0ull < check_msg.index());
    auto ret = Step(check_msg);
    if (ErrorCode::OK != ret) {
        logerr("Step selfid %" PRIu64 " index %" PRIu64 " ret %d", 
                check_msg.to(), check_msg.index(), ret);
    }

    return ret;
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

bool Paxos::WaitFor(
        uint64_t index, const std::chrono::milliseconds timeout)
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

uint64_t Paxos::GetSelfId() const 
{
    return paxos_impl_->GetSelfId();
}

uint64_t Paxos::GetLogId() const
{
    return paxos_impl_->GetLogId();
}

bool Paxos::IsChosen(uint64_t index) 
{
    assert(0ull < index);
    std::lock_guard<std::mutex> lock(paxos_mutex_);
    return index <= paxos_impl_->GetCommitedIndex(); 
}

int Paxos::CheckChosen(uint64_t index, uint64_t eid)
{
    assert(0ull < index);
    assert(0ull < eid);
    std::lock_guard<std::mutex> lock(paxos_mutex_);
    if (index > paxos_impl_->GetCommitedIndex()) {
        return -1;
    }

    auto ins = paxos_impl_->GetInstance(index, false);
    if (nullptr == ins) {
        return -2;
    }

    assert(nullptr != ins);
    assert(true == ins->IsChosen());
    if (ins->GetAcceptedValue().eid() != eid) {
        return 0;
    }

    return 1;
}

std::unique_ptr<SnapshotMetadata> Paxos::CreateSnapshotMetadata()
{
    std::lock_guard<std::mutex> lock(paxos_mutex_);
    assert(nullptr != paxos_impl_);
    
    auto meta = cutils::make_unique<SnapshotMetadata>();
    assert(nullptr != meta);

    meta->set_logid(paxos_impl_->GetLogId());
    meta->set_max_index(paxos_impl_->GetMaxIndex());
    meta->set_commited_index(paxos_impl_->GetCommitedIndex());
    auto conf_state = meta->mutable_conf_state();
    assert(nullptr != conf_state);

    for (auto node_id : paxos_impl_->GetGroupIds()) {
        conf_state->add_nodes(node_id);
    }

    assert(conf_state->nodes_size() == paxos_impl_->GetGroupIds().size());
    return meta;
}

} // namespace paxos


