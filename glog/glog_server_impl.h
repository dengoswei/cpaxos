#pragma once


namespace paxos {
    namespace proto {
        class PaxosMsg;
    }
}

namespace glog {
    class Status;
    class ServerContext;
    class NoopMsg;
}

namespace glog {


class GlogServerImpl final : public Glog::Service {

public:
    
    GlogServerImpl(); // TODO


    grpc::Status PostMsg(
            grpc::ServerContext* context, 
            const paxos::proto::PaxosMsg* request, 
            glog::NoopMsg* reply) override;

private:
    paxos::Paxos paxos_log_;
}; 



}  // namespace glog


