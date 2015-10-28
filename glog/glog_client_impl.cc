
#include "glog_client_impl.h"
#include "utils.h"



using paxos::proto::PaxosMsg;
using grpc::ClientContext;
using grpc::Status;

namespace glog {

void GlogClientImpl::PostMsg(
        const uint64_t index, const std::unique_ptr<paxos::Message>& msg)
{
    PaxosMsg request;
    {
        // init
        request.set_type(static_cast<int>(msg.type));
        request.set_from_id(msg.from_id);
        request.set_to_id(msg.to_id);
        assert(msg.to_id == selfid_);
        request.set_index(index);
        request.set_proposed_num(msg.prop_num);
        if (0 != msg.promised_num) {
            request.set_promised_num(msg.promised_num);
            if (0 != msg.accepted_num) {
                request.set_accepted_num(msg.accepted_num);
                request.set_accepted_value(msg.accepted_value);
            }
        }
    }

    NoopMsg reply;

    ClientContext context;
    Status status = stub_->PostMsg(&context, request, reply);
    if (status.ok()) {
        logdebug("index %" PRIu64 " PostMsg succ", index);
    } else {
        logerr("index %" PRIu64 " PostMsg failed", index);
    }

    return ;
}




} // namespace glog



