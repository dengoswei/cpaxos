#pragma once

#include <set>
#include <stdint.h>
#include <vector>
#include <memory>
#include <map>
#include <deque>
#include <mutex>


namespace paxos {


class Message;
class HardState;
class PaxosImpl;

}


namespace test {

extern uint64_t LOGID;
extern std::set<uint64_t> GROUP_IDS;


std::vector<std::unique_ptr<paxos::Message>>
apply(
    std::map<
        uint64_t, std::unique_ptr<paxos::PaxosImpl>>& map_paxos, 
    const std::vector<std::unique_ptr<paxos::Message>>& vec_input_msg);

void apply_until(
    std::map<uint64_t, std::unique_ptr<paxos::PaxosImpl>>& map_paxos, 
    std::vector<std::unique_ptr<paxos::Message>>&& vec_msg);


std::map<uint64_t, std::unique_ptr<paxos::PaxosImpl>>
    build_paxos(
            uint64_t logid,  
            const std::set<uint64_t>& group_ids);

class StorageHelper {

public:
    int write(std::unique_ptr<paxos::HardState> hs);

    std::unique_ptr<paxos::HardState> 
        read(uint64_t logid, uint64_t log_index);

private:
    std::mutex mutex_;
    std::map<std::string, std::unique_ptr<paxos::HardState>> logs_;
};

class SendHelper {

public:
    int send(std::unique_ptr<paxos::Message> msg);

private:
    std::mutex queue_mutex_;
    std::deque<std::unique_ptr<paxos::Message>> msg_queue_;
};



std::unique_ptr<paxos::Message> 
buildMsgProp(uint64_t logid, uint64_t to, uint64_t index);


} // namespace test


