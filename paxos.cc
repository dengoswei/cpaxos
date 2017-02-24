#include <deque>
#include <algorithm>
#include "paxos.h"
#include "mem_utils.h"
#include "log_utils.h"
#include "hassert.h"

using namespace std;

namespace paxos {

std::chrono::milliseconds PAXOS_TIMEOUT{100};


Paxos::Paxos(
        uint64_t logid, 
        uint64_t selfid, 
        uint32_t timeout, 
        const std::set<uint64_t>& group_ids, 
        PaxosCallBack callback)
    : timeout_(timeout)
    , paxos_impl_(cutils::make_unique<PaxosImpl>(logid, selfid, group_ids))
    , callback_(callback)
{
    assert(nullptr != callback_.read);
    assert(nullptr != callback_.write);
    assert(nullptr != callback_.send);
}

Paxos::Paxos(
        uint64_t selfid, 
        uint32_t timeout, 
        const paxos::SnapshotMetadata& meta, 
        PaxosCallBack callback)
    : timeout_(timeout)
    , paxos_impl_(nullptr)
    , callback_(callback)
{
    assert(nullptr != callback_.read);
    assert(nullptr != callback_.write);
    assert(nullptr != callback_.send);

    std::set<uint64_t> group_ids;
    {
        assert(true == meta.has_conf_state());
        auto& conf_state = meta.conf_state();
        for (int idx = 0; idx < conf_state.nodes_size(); ++idx) {
            group_ids.insert(conf_state.nodes(idx));
        }
    }

    assert(false == group_ids.empty());
    assert(true == meta.has_logid());
    if (false == meta.has_commited_index()) {
        // new plog
        paxos_impl_ = cutils::make_unique<
            PaxosImpl>(meta.logid(), selfid, group_ids);
        assert(nullptr != paxos_impl_);
        assert(0 == paxos_impl_->GetMaxIndex());
        assert(0 == paxos_impl_->GetCommitedIndex());
    }
    else {
        assert(true == meta.has_commited_index());
        int err = 0;
        std::unique_ptr<paxos::HardState> hs = nullptr;
        uint64_t index = meta.commited_index();
		index = max(uint64_t{1}, index);

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

		assert(1 <= index);
		assert(index > meta.commited_index());
		--index;
        assert(index >= meta.commited_index());
        // TODO
        paxos_impl_ = cutils::make_unique<PaxosImpl>(
                meta.logid(), selfid, group_ids, 
                hs_deque, meta.commited_index() == index);
        assert(nullptr != paxos_impl_);
        assert(index == paxos_impl_->GetMaxIndex());
        assert(index >= paxos_impl_->GetCommitedIndex());
        assert(meta.commited_index() <= paxos_impl_->GetCommitedIndex());
    }

    assert(nullptr != paxos_impl_);
}


Paxos::~Paxos() = default;

std::tuple<paxos::ErrorCode, uint64_t>
Paxos::Propose(
        const uint64_t index, 
        const uint64_t reqid, 
        const std::string& proposing_value)
{
    Message prop_msg;
    prop_msg.set_type(MessageType::BEGIN_PROP);

    {
        auto prop_entry = prop_msg.mutable_accepted_value();
        assert(nullptr != prop_entry);
        prop_entry->set_type(EntryType::EntryNormal);
        prop_entry->set_data(proposing_value);
        prop_entry->set_reqid(reqid);
    }

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
        // prop_msg.set_proposed_num(paxos_impl_->GetProposeNum());
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
    // TODO
    std::vector<std::unique_ptr<HardState>> vec_hs;
    std::vector<std::unique_ptr<Message>> vec_msg;

    uint64_t index = msg.index();
//    hassert(0 < index, "index %" PRIu64 " type %d peer %" 
//            PRIu64 " to %" PRIu64, 
//            msg.index(), static_cast<int>(msg.type()), 
//            msg.from(), msg.to());
//    assert(0 < index);

    {
        // 1.
        unique_lock<mutex> lock(paxos_mutex_);
        assert(msg.logid() == paxos_impl_->GetLogId());
        prev_commit_index = paxos_impl_->GetCommitedIndex();

        unique_ptr<PaxosInstance> disk_ins = nullptr;
        if (0 < index && index < paxos_impl_->GetCommitedIndex()) {
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

        std::unique_ptr<HardState> hs = nullptr;
        if (0 < index && nullptr == disk_ins.get()) {
            auto ins = paxos_impl_->GetInstance(index, false);
            if (nullptr != ins) {
                hs = ins->GetPendingHardState(paxos_impl_->GetLogId(), index);
            }
        }

        vector<unique_ptr<Message>> vec_try_prop_msg;
        tie(vec_hs, vec_try_prop_msg) = 
            CheckTimeoutNoLock(msg.index(), timeout_);

        if (nullptr != hs) {
            vec_hs.push_back(move(hs));
        }

        if (false == vec_try_prop_msg.empty()) {
            vec_msg.reserve(vec_msg.size() + vec_try_prop_msg.size());
            for_each(
                    vec_try_prop_msg.begin(), 
                    vec_try_prop_msg.end(), 
                    [&](unique_ptr<Message>& try_prop_msg) {
                        assert(nullptr != try_prop_msg);
                        vec_msg.push_back(move(try_prop_msg));
                        assert(nullptr == try_prop_msg);
                    });
            vec_try_prop_msg.clear();
        }
    }

    // 2.
    int ret = 0;
    if (false == vec_hs.empty()) {
        ret = callback_.write(vec_hs);
        if (0 != ret) {
            logerr("callback_.write index %" PRIu64 " vec_hs.size %zu ret %d", 
                    msg.index(), vec_hs.size(), ret);
            return paxos::ErrorCode::STORAGE_WRITE_ERROR;
        }
    }

    assert(0 == ret);
    if (false == vec_msg.empty()) {
        ret = callback_.send(move(vec_msg));
        if (0 != ret) {
            logdebug("callback_.send index %" PRIu64 " vec_msg.size %zu ret %d", 
                    msg.index(), vec_msg.size(), ret);
        }
        assert(true == vec_msg.empty());
    }

    bool update = false;
    {
        std::lock_guard<std::mutex> lock(paxos_mutex_);
        paxos_impl_->CommitStep(index, vec_hs);
        // paxos_impl_->CommitStep(index, store_seq);
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

std::tuple<
    std::vector<std::unique_ptr<paxos::HardState>>, 
    std::vector<std::unique_ptr<paxos::Message>>>
Paxos::CheckTimeoutNoLock(
        uint64_t exclude_index, const std::chrono::milliseconds& timeout)
{
    vector<unique_ptr<HardState>> vec_hs;
    vector<unique_ptr<Message>> vec_msg;
    for (uint64_t index = paxos_impl_->GetCommitedIndex() + 1;
            index <= paxos_impl_->GetMaxIndex(); ++index) {
        if (index == exclude_index) {
            continue;
        }

        auto ins = paxos_impl_->GetInstance(index, false);
        if (nullptr != ins) {
            // check timeout
            if (true == ins->IsChosen() ||
                    false == ins->IsTimeout(timeout)) {
                continue;
            }
            // else => timeout
        }
        else {
            assert(nullptr == ins);
            ins = paxos_impl_->GetInstance(index, true);
            assert(nullptr != ins);
        }

        assert(nullptr != ins);
        Message try_prop_msg;
        try_prop_msg.set_type(MessageType::TRY_PROP);
        {
            auto entry = try_prop_msg.mutable_accepted_value();
            assert(nullptr != entry);
            entry->set_type(EntryType::EntryNormal);
        }

        try_prop_msg.set_logid(paxos_impl_->GetLogId());
        try_prop_msg.set_to(paxos_impl_->GetSelfId());
        try_prop_msg.set_index(index);

        auto rsp_msg_type = 
            ::paxos::Step(*paxos_impl_, try_prop_msg, nullptr);
        auto new_vec_msg = ProduceRsp(
                *paxos_impl_, try_prop_msg, rsp_msg_type, nullptr);
        logerr("TIMEOUT!! rsp_msg_type %d new_vec_msg.size %zu", 
                static_cast<int>(rsp_msg_type), new_vec_msg.size());
        hassert(false == new_vec_msg.empty(), "commited_index %" PRIu64 
                " max_index %" PRIu64 " rsp_msg_type %d new_vec_msg.size %zu", 
                paxos_impl_->GetCommitedIndex(), paxos_impl_->GetMaxIndex(),
                static_cast<int>(rsp_msg_type), new_vec_msg.size());

        vec_msg.reserve(vec_msg.size() + new_vec_msg.size());
        for_each(new_vec_msg.begin(), new_vec_msg.end(), 
                [&](std::unique_ptr<Message>& msg) {
                    assert(nullptr != msg);
                    vec_msg.push_back(move(msg));
                    assert(nullptr == msg);
                });

        auto hs = ins->GetPendingHardState(paxos_impl_->GetLogId(), index);
        assert(nullptr != hs);
        vec_hs.push_back(move(hs));
        assert(nullptr == hs);
    }

    return make_tuple(move(vec_hs), move(vec_msg));
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

int Paxos::CheckChosen(uint64_t index, uint64_t reqid)
{
    assert(0ull < index);
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
    auto& accepted_value = ins->GetAcceptedValue();
    logdebug("index %" PRIu64 " reqid %" PRIu64 
            " accepted_value: reqid %" PRIu64 " value.size %zu", 
            index, reqid, accepted_value.reqid(), 
            accepted_value.data().size());
    if (accepted_value.reqid() != reqid) {
        return 0;
    }

    return 1;
}

std::unique_ptr<SnapshotMetadata> Paxos::CreateSnapshotMetadata()
{
    auto meta = cutils::make_unique<SnapshotMetadata>();
    assert(nullptr != meta);

    auto conf_state = meta->mutable_conf_state();
    assert(nullptr != conf_state);

    std::lock_guard<std::mutex> lock(paxos_mutex_);
    assert(nullptr != paxos_impl_);
    meta->set_logid(paxos_impl_->GetLogId());
    meta->set_commited_index(paxos_impl_->GetCommitedIndex());

    for (auto node_id : paxos_impl_->GetGroupIds()) {
        conf_state->add_nodes(node_id);
    }

    assert(static_cast<size_t>(
				conf_state->nodes_size()) == paxos_impl_->GetGroupIds().size());
    return meta;
}

} // namespace paxos


