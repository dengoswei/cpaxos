#pragma once

#include <memory>
#include "glog.grpc.pb.h"
#include "paxos.h"


namespace paxos {
    class Paxos;
    class Message;
}

namespace glog {
    class Status;
    class ServerContext;
    class NoopMsg;
}

namespace glog {


class GlogServiceImpl final : public Glog::Service {

public:
    
    GlogServiceImpl(
            const std::map<uint64_t, std::string>& groups, 
            std::unique_ptr<paxos::Paxos>&& paxos_log, 
            paxos::Callback callback); 

    ~GlogServiceImpl();


    grpc::Status PostMsg(
            grpc::ServerContext* context, 
            const paxos::Message* request, glog::NoopMsg* reply) override;

    grpc::Status Propose(
            grpc::ServerContext* context, 
            const glog::ProposeRequest* request, 
            glog::ProposeResponse* reply)    override;

    // internal use
    grpc::Status GetPaxosInfo(
            grpc::ServerContext* context, 
            const glog::NoopMsg* request, 
            glog::PaxosInfo* reply) override;

    grpc::Status TryCatchUp(
            grpc::ServerContext* context, 
            const glog::NoopMsg* request, 
            glog::NoopMsg* reply) override;

    // test
    grpc::Status GetGlog(
            grpc::ServerContext* context, 
            const glog::GetGlogRequest* request, 
            glog::GetGlogResponse* reply)    override;

private:
    std::map<uint64_t, std::string> groups_;
    std::unique_ptr<paxos::Paxos> paxos_log_;
    paxos::Callback callback_;
}; 



}  // namespace glog


