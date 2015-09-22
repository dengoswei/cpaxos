#include "paxos_job_impl.h"


namespace {

class SimpleCallback {

public:
    int operator()(
            uint64_t index, 
            const std::unique_ptr<paxos::proto::HardState>& hs, 
            const std::unique_ptr<paxos::Message>& rsp_msg) 
    {
        if (nullptr != hs) {
            assert(index == hs->index());
            // TODO: save
        }

        if (nullptr != rsp_msg) {
            if (0 != rsp_msg->peer_id) {
                
            } else {
                assert(0 == rsp_msg->peer_id);
                for (auto& pv : map_peers_) {

                }
                // broadcast
            }
        }
        // TODO
        return 0;
    }

private:
    int callPost(const std::string& peerAddr, const paxos::Message& rsp_msg)
    {

        // TODO
    }

private:
    // TODO: 
    // 1. leveldb / memdb(std::map)
    // 2. Paxos::Client // 
    std::map<uint64_t, std::string> map_peers_;
};


} // namespace 


namespace paxos {






} // namespace paxos



