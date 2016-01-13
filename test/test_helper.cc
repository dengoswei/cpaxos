#include "test_helper.h"
#include "paxos.pb.h"
#include "paxos_impl.h"
#include "paxos_instance.h"


using namespace std;
using namespace paxos;

namespace test {

uint64_t LOGID = 1ull;
std::set<uint64_t> GROUP_IDS{1ull, 2ull, 3ull};


std::vector<std::unique_ptr<paxos::Message>>
apply(
    std::map<
        uint64_t, std::unique_ptr<paxos::PaxosImpl>>& map_paxos, 
    const std::vector<std::unique_ptr<paxos::Message>>& vec_input_msg)
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
        for (auto& rsp_msg : vec_rsp_msg) {
            assert(nullptr != rsp_msg);
            assert(0ull != rsp_msg->to());
            assert(0ull != rsp_msg->from());
            logdebug("INFO msg.type %d rsp_msg_type %d", 
                    static_cast<int>(msg->type()), 
                    static_cast<int>(rsp_msg_type));
            vec_msg.emplace_back(move(rsp_msg));
            assert(nullptr == rsp_msg);
        }

        // clear pending ins
        {
            auto ins = paxos->GetInstance(msg->index(), false);
            assert(nullptr != ins);
            auto hs = ins->GetPendingHardState(
                    paxos->GetLogId(), msg->index());

            auto store_seq = nullptr == hs ? 0 : hs->seq();
            paxos->CommitStep(msg->index(), store_seq);
        }
    }

    return vec_msg;
}

void apply_until(
    std::map<uint64_t, std::unique_ptr<paxos::PaxosImpl>>& map_paxos, 
    std::vector<std::unique_ptr<paxos::Message>>&& vec_msg)
{
    auto count = 0;
    while (false == vec_msg.empty()) {
        ++count;
        logdebug("APPLY INFO count %d vec_msg.size %zu", 
                count, vec_msg.size());
        vec_msg = apply(map_paxos, vec_msg);
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

} // namespace test



