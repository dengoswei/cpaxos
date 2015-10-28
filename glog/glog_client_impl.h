#pragma once

#include <memory>
#include <grpc++/grpc++.h>

namespace paxos {
    struct Message;
} // namespace paxos


namespace glog {

class GlogClientImpl {


public:
    GlogClientImpl(
            const uint64_t selfid, 
            std::shared_ptr<grpc::Channel> channel)
        : selfid_(selfid)
        , stub_(glog::Glog::NewStub(channel)) {
        // 
    }

    void PostMsg(
            const uint64_t index, 
            const std::unique_ptr<paxos::Message>& paxos_msg);

private:
    uint64_t selfid_;
    std::unique_ptr<glog::Glog::Stub> stub_;
};



} // namespace glog



