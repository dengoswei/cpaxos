#pragma once

#include <tuple>
#include <memory>
#include <grpc++/grpc++.h>
#include "gsl.h"
#include "glog.grpc.pb.h"

namespace paxos {
    class Message;
} // namespace paxos


namespace glog {



class GlogClientImpl {


public:
    GlogClientImpl(
            const uint64_t svrid, 
            std::shared_ptr<grpc::Channel> channel);

    ~GlogClientImpl();

    void PostMsg(const paxos::Message& msg);

    int Propose(gsl::cstring_view<> data);

    std::tuple<int, uint64_t, uint64_t> GetPaxosInfo();

    void TryCatchUp();

    std::tuple<std::string, std::string> GetGlog(uint64_t index);

private:
    uint64_t svrid_;
    std::unique_ptr<glog::Glog::Stub> stub_;
};

class GlogAsyncClientImpl {

public:
    GlogAsyncClientImpl(
            const uint64_t selfid, 
            std::shared_ptr<grpc::Channel> channel);

    ~GlogAsyncClientImpl();

    void PostMsg(const paxos::Message& msg);

    GlogAsyncClientImpl(GlogAsyncClientImpl&&) = delete;
    GlogAsyncClientImpl(const GlogAsyncClientImpl&) = delete;

private:
    void gc();

private:
    uint64_t selfid_;
    std::unique_ptr<glog::Glog::Stub> stub_;

    uint64_t rpc_seq_ = 0;
    grpc::CompletionQueue cq_;
    std::map<uint64_t, 
        std::tuple<
            std::unique_ptr<grpc::ClientAsyncResponseReader<NoopMsg>>, 
            grpc::Status, 
            NoopMsg>> rsp_map_;
};


} // namespace glog



