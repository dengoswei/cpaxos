#include "glog_server_impl.h"


namespace glog {

grpc::Status GlogServerImpl::PostMsg(
        grpc::ServerContext* context, 
        const paxos::proto::PaxosMsg* request, 
        glog::NoopMsg* reply) override
{
    assert(nullptr != context);
    assert(nullptr != request);
    assert(nullptr != reply);
    paxos::Message msg;
    {
        // init
        msg.type = static_cast<paxos::MessageType>(request.type);
        msg.peer_id = request->from_id(); 
        msg.to_id = request->to_id(); 
        assert(msg.to_id == paxos_log_.GetSelfId());
        msg.prop_num = request->prop_num();
        msg.promised_num = request->promised_num();
        if (0 != request->accepted_num()) {
            msg.accepted_num = request->accepted_num();
            msg.accepted_value = request->accepted_value();
        }
    }

    int ret = paxos_log_.Step(request->index(), msg, callback_);
    if (0 != ret) {
        logerr("paxos_log.Step index %" PRIu64 " failed ret %d", 
                request->index(), ret);

        return Status(ret, "paxos log Step failed");
    }

    return Status::OK;
}


} // namespace glog


