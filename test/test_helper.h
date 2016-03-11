#pragma once

#include <set>
#include <stdint.h>
#include <vector>
#include <memory>
#include <map>
#include <deque>
#include <mutex>
#include <tuple>


namespace paxos {

class Entry;
class Message;
class HardState;
class PaxosImpl;
class Paxos;

}


namespace test {

extern uint64_t LOGID;
extern std::set<uint64_t> GROUP_IDS;

class SendHelper;
class StorageHelper;

std::vector<std::unique_ptr<paxos::Message>>
apply(
    std::map<
        uint64_t, std::unique_ptr<paxos::PaxosImpl>>& map_paxos, 
    const std::vector<std::unique_ptr<paxos::Message>>& vec_input_msg, 
    int disk_fail_ratio, 
    int drop_ratio);

void apply_until(
    std::map<uint64_t, std::unique_ptr<paxos::PaxosImpl>>& map_paxos, 
    std::vector<std::unique_ptr<paxos::Message>>&& vec_msg, 
    int disk_fail_ratio, 
    int drop_ratio);


std::map<uint64_t, std::unique_ptr<paxos::PaxosImpl>>
    build_paxos(
            uint64_t logid,  
            const std::set<uint64_t>& group_ids);

std::tuple<
    std::map<uint64_t, std::unique_ptr<test::StorageHelper>>, 
    std::map<uint64_t, std::unique_ptr<paxos::Paxos>>>
build_paxos(
            uint64_t logid, 
            const std::set<uint64_t>& group_ids, 
            SendHelper& sender, int disk_fail_ratio);

class StorageHelper {

public:
    StorageHelper(int disk_fail_ratio)
        : disk_fail_ratio_(disk_fail_ratio)
    {

    }

    int write(std::unique_ptr<paxos::HardState> hs);

    std::unique_ptr<paxos::HardState> 
        read(uint64_t logid, uint64_t log_index);

private:
    std::unique_ptr<paxos::HardState>
        read_nolock(const std::string& key) const;

private:
    const int disk_fail_ratio_;
    std::mutex mutex_;
    std::map<std::string, std::unique_ptr<paxos::HardState>> logs_;
};

class SendHelper {

public:
    SendHelper(int drop_ratio)
        : drop_ratio_(drop_ratio)
    {

    }

    int send(std::unique_ptr<paxos::Message> msg);

    size_t apply(
            std::map<uint64_t, std::unique_ptr<paxos::Paxos>>& map_paxos);

    int apply_until(
            std::map<uint64_t, std::unique_ptr<paxos::Paxos>>& map_paxos);

    bool empty();

private:
    const int drop_ratio_;
    std::mutex queue_mutex_;
    std::deque<std::unique_ptr<paxos::Message>> msg_queue_;
};



std::unique_ptr<paxos::Message> 
buildMsgProp(uint64_t logid, uint64_t to, uint64_t index);

std::string genPropValue();

void set_accepted_value(
        const std::string& value, paxos::Message& msg);

void set_accepted_value(
        const paxos::Entry& value, paxos::Message& msg);

void set_accepted_value(
        const paxos::Entry& value, paxos::HardState& hs);




} // namespace test


