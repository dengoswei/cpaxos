#include "test_helper.h"
#include "paxos.pb.h"
#include "paxos.h"
#include "paxos_impl.h"
#include "paxos_instance.h"
#include "utils.h"


using namespace std;
using namespace paxos;

namespace test {

uint64_t LOGID = 1ull;
std::set<uint64_t> GROUP_IDS{1ull, 2ull, 3ull};

inline bool btest(int ratio)
{
    return 0 >= ratio ? 
        false : 100 <= ratio ? 
            true : random_int(0, 100) < ratio;
}

std::vector<std::unique_ptr<paxos::Message>>
apply(
    std::map<
        uint64_t, std::unique_ptr<paxos::PaxosImpl>>& map_paxos, 
    const std::vector<std::unique_ptr<paxos::Message>>& vec_input_msg, 
    int disk_fail_ratio, 
    int drop_ratio)
{
    vector<unique_ptr<Message>> vec_msg;
    for (const auto& msg : vec_input_msg) {
        assert(nullptr != msg);
        assert(map_paxos.end() != map_paxos.find(msg->to()));

        auto& paxos = map_paxos[msg->to()];
        if (nullptr == paxos) {
            logdebug("DROP msg:type %d from %" PRIu64 " to %" PRIu64, 
                    static_cast<int>(msg->type()), 
                    msg->from(), msg->to());
            continue;
        }

        assert(nullptr != paxos);
        // no disk_ins inv
        auto rsp_msg_type = Step(*paxos, *msg, nullptr);
        auto vec_rsp_msg = 
            ProduceRsp(*paxos, *msg, rsp_msg_type, nullptr);

        // clear pending ins
        bool disk_fail = btest(disk_fail_ratio);
        if (false == disk_fail) {
            auto ins = paxos->GetInstance(msg->index(), false);
            assert(nullptr != ins);
            auto hs = ins->GetPendingHardState(
                    paxos->GetLogId(), msg->index());

            auto store_seq = nullptr == hs ? 0 : hs->seq();
            paxos->CommitStep(msg->index(), store_seq);

            for (auto& rsp_msg : vec_rsp_msg) {
                assert(nullptr != rsp_msg);
                assert(0ull != rsp_msg->to());
                assert(0ull != rsp_msg->from());

                uint8_t prop_id = 0;
                uint64_t prop_cnt = 0ull;
                tie(prop_id, prop_cnt) = 
                    prop_num_decompose(rsp_msg->proposed_num());
                logdebug("INFO msg.type %d rsp_msg_type %d"
                        " index %" PRIu64 " from %" PRIu64 " to %" PRIu64
                        " proposed_num %" PRIu64 "(%d:%" PRIu64 ")", 
                        static_cast<int>(msg->type()), 
                        static_cast<int>(rsp_msg_type), 
                        rsp_msg->index(), rsp_msg->from(), rsp_msg->to(), 
                        rsp_msg->proposed_num(), 
                        static_cast<int>(prop_id), prop_cnt);

                bool drop = btest(drop_ratio);
                if (false == drop) {
                    vec_msg.emplace_back(move(rsp_msg));
                    assert(nullptr == rsp_msg);
                }
                else {
                    logdebug("DROP from %" PRIu64 " to %" PRIu64
                            " proposed_num %" PRIu64 " msg.type %d", 
                            rsp_msg->from(), rsp_msg->to(), 
                            rsp_msg->proposed_num(), 
                            static_cast<int>(rsp_msg->type()));
                }
            }
        }
        else {
            logdebug("DISK FAIL id %" PRIu64 " index %" PRIu64 
                    " vec_rsp_msg.size %zu", 
                    msg->to(), msg->index(), vec_rsp_msg.size());
        }
    }

    return vec_msg;
}

void apply_until(
    std::map<uint64_t, std::unique_ptr<paxos::PaxosImpl>>& map_paxos, 
    std::vector<std::unique_ptr<paxos::Message>>&& vec_msg, 
    int disk_fail_ratio, 
    int drop_ratio)
{
    auto count = 0;
    while (false == vec_msg.empty()) {
        ++count;
        logdebug("APPLY INFO count %d vec_msg.size %zu", 
                count, vec_msg.size());
        vec_msg = apply(map_paxos, vec_msg, disk_fail_ratio, drop_ratio);
    }
}


std::map<uint64_t, std::unique_ptr<paxos::PaxosImpl>>
    build_paxos(
            uint64_t logid,  
            const std::set<uint64_t>& group_ids)
{
    map<uint64_t, unique_ptr<PaxosImpl>> map_paxos;
    for (auto id : group_ids) {
        auto paxos = make_unique<PaxosImpl>(logid, id, group_ids);
        assert(nullptr != paxos);
        assert(paxos->GetSelfId() == id);
        assert(paxos->GetLogId() == logid);
        assert(map_paxos.end() == map_paxos.find(id));
        map_paxos[id] = move(paxos);
        assert(nullptr == paxos);
    }

    return map_paxos;
}


std::unique_ptr<paxos::Message> 
buildMsgProp(uint64_t logid, uint64_t to, uint64_t index)
{
    auto msg = make_unique<Message>();
    assert(nullptr != msg);

    msg->set_logid(logid);
    msg->set_type(MessageType::BEGIN_PROP);
    msg->set_to(to);
    msg->set_index(index);

    RandomStrGen<100, 200> gen;
    msg->set_accepted_value(gen.Next());
    return msg;
}


// StorageHelper

std::string makeKey(uint64_t logid, uint64_t log_index)
{
    string key(sizeof(uint64_t) * 2, 0);
    assert(key.size() == sizeof(uint64_t) * 2);

    memcpy(&key[0], &logid, sizeof(uint64_t));
    memcpy(&key[0] + sizeof(uint64_t), &log_index, sizeof(uint64_t));
    return key;
}

int StorageHelper::write(std::unique_ptr<paxos::HardState> hs)
{
    assert(nullptr != hs);
    assert(0ull < hs->index());
    assert(0ull < hs->proposed_num());

    auto key = makeKey(hs->logid(), hs->index());
    lock_guard<mutex> lock(mutex_);
    if (logs_.end() != logs_.find(key)) {
        const auto& prev_hs = logs_.at(key);
        assert(nullptr != prev_hs);
        if (prev_hs->seq() >= hs->seq()) {
            return 0; // don't over-write;
        }
    }

    logs_[key] = move(hs);
    assert(nullptr == hs);
    assert(nullptr != logs_.at(key));
    return 0;
}

std::unique_ptr<paxos::HardState>
StorageHelper::read_nolock(const std::string& key) const
{
    assert(false == key.empty());
    if (logs_.end() == logs_.find(key)) {
        return nullptr;
    }

    assert(nullptr != logs_.at(key));
    return make_unique<HardState>(*(logs_.at(key)));
}

std::unique_ptr<paxos::HardState>
StorageHelper::read(uint64_t logid, uint64_t log_index)
{
    assert(0ull < log_index);
    auto key = makeKey(logid, log_index);
    lock_guard<mutex> lock(mutex_);
    return read_nolock(key);
}

int SendHelper::send(std::unique_ptr<paxos::Message> msg)
{
    assert(nullptr != msg);
    lock_guard<mutex> lock(queue_mutex_);
    msg_queue_.push_back(move(msg));
    assert(nullptr == msg);
    return 0;
}

size_t SendHelper::apply(
        std::map<uint64_t, std::unique_ptr<paxos::Paxos>>& map_paxos)
{
    deque<std::unique_ptr<paxos::Message>> prev_msg_queue;
    {
        lock_guard<mutex> lock(queue_mutex_);
        prev_msg_queue.swap(msg_queue_);
    }

    for (auto& msg : prev_msg_queue) {
        assert(nullptr != msg);
        assert(map_paxos.end() != map_paxos.find(msg->to()));
        auto& paxos = map_paxos[msg->to()];
        if (nullptr == paxos) {
            logdebug("DROP msg from %" PRIu64 " to %" PRIu64 
                    " msg.type %d", 
                    msg->from(), msg->to(), 
                    static_cast<int>(msg->type()));
            continue;
        }
        assert(nullptr != paxos);

        auto ret_code = paxos->Step(*msg);
        if (ErrorCode::OK != ret_code) {
            logerr("APPLY_ERROR step msg from %" PRIu64 " to %" PRIu64
                    " index %" PRIu64 " type %d", 
                    msg->from(), msg->to(), msg->index(), 
                    static_cast<int>(msg->type()));
        }
    }

    return prev_msg_queue.size();
}

int SendHelper::apply_until(
        std::map<uint64_t, std::unique_ptr<paxos::Paxos>>& map_paxos)
{
    int count = 0;
    while (false == empty()) {
        ++count;
        auto apply_count = apply(map_paxos);
        logdebug("APPLY_INFO count %d apply_count %zu", 
                count, apply_count);
    }

    return count;
}

bool SendHelper::empty()
{
    lock_guard<mutex> lock(queue_mutex_);
    return msg_queue_.empty();
}


std::tuple<
    std::map<uint64_t, std::unique_ptr<test::StorageHelper>>, 
    std::map<uint64_t, std::unique_ptr<paxos::Paxos>>>
build_paxos(
        uint64_t logid, const std::set<uint64_t>& group_ids, 
        SendHelper& sender, 
        int disk_fail_ratio)
{
    map<uint64_t, unique_ptr<StorageHelper>> map_storage;
    map<uint64_t, std::unique_ptr<paxos::Paxos>> map_paxos;

    for (auto id : group_ids) {
        assert(map_storage.end() == map_storage.find(id));
        assert(map_paxos.end() == map_paxos.find(id));

        auto ustorage = make_unique<StorageHelper>(disk_fail_ratio);
        assert(nullptr != ustorage);
        auto storage = ustorage.get();
        assert(nullptr != storage);

        PaxosCallBack callback;
        callback.read = 
            [=](uint64_t logid, uint64_t index) 
                -> std::unique_ptr<HardState> {
            
                return storage->read(logid, index);
            };

        callback.write = 
            [=](std::unique_ptr<HardState> hs) -> int {
                return storage->write(move(hs));
            };

        callback.send = 
            [&](std::unique_ptr<Message> msg) -> int {
                return sender.send(move(msg));
            };

        auto paxos = make_unique<Paxos>(logid, id, group_ids, callback);
        assert(nullptr != paxos);

        map_storage[id] = move(ustorage);
        map_paxos[id] = move(paxos);

        assert(nullptr == ustorage);
        assert(nullptr == paxos);
    }

    assert(map_paxos.size() == group_ids.size());
    assert(map_storage.size() == group_ids.size());
    return make_tuple(move(map_storage), move(map_paxos));
}

} // namespace test



