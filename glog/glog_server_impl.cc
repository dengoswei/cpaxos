#include "glog_server_impl.h"
#include "paxos.h"
#include "paxos.pb.h"
#include "utils.h"


using namespace std;

namespace glog {

GlogServiceImpl::GlogServiceImpl(
        std::unique_ptr<paxos::Paxos>&& paxos_log, 
        paxos::Callback callback)
    : paxos_log_(move(paxos_log))
    , callback_(move(callback))
{
    assert(nullptr != paxos_log_);
    assert(nullptr != callback_);
}

GlogServiceImpl::~GlogServiceImpl() = default;

grpc::Status 
GlogServiceImpl::PostMsg(
        grpc::ServerContext* context, 
        const paxos::Message* request, glog::NoopMsg* reply)
{
    assert(nullptr != context);
    assert(nullptr != request);
    assert(nullptr != reply);

    logdebug("PostMsg msg index %" PRIu64 " type %u peer_id %" PRIu64 " to_id %" PRIu64 " ", 
            request->index(), static_cast<uint32_t>(request->type()), 
            request->peer_id(), request->to_id());
    const paxos::Message& msg = *request;
    int ret = paxos_log_->Step(request->index(), msg, callback_);
    if (0 != ret) {
        logerr("paxos_log.Step selfid %" PRIu64 " index %" PRIu64 " failed ret %d", 
                paxos_log_->GetSelfId(), request->index(), ret);

        return grpc::Status(
                static_cast<grpc::StatusCode>(ret), "paxos log Step failed");
    }

    return grpc::Status::OK;
}

grpc::Status
GlogServiceImpl::Propose(
        grpc::ServerContext* context, 
        const glog::ProposeRequest* request, glog::ProposeResponse* reply)
{
    assert(nullptr != context);
    assert(nullptr != request);
    assert(nullptr != reply);

    {
        string peer = context->peer();
        logdebug("Propose datasize %" PRIu64 " peer %s", 
                request->data().size(), peer.c_str());
    }
    int retcode = 0;
    uint64_t index = 0;
    tie(retcode, index) = paxos_log_->Propose(
            {request->data().data(), request->data().size()}, callback_);
    reply->set_retcode(retcode);
    if (0 != retcode) {
        logerr("Propose retcode %d", retcode);
        return grpc::Status(
                static_cast<grpc::StatusCode>(retcode), "propose failed");
    }

    hassert(0 < index, "index %" PRIu64 "\n", index); 
    paxos_log_->Wait(index);
    return grpc::Status::OK;
}

grpc::Status
GlogServiceImpl::GetGlog(
        grpc::ServerContext* context, 
        const glog::GetGlogRequest* request, 
        glog::GetGlogResponse* reply)
{
    assert(nullptr != context);
    assert(nullptr != request);
    assert(nullptr != reply);

    {
        string peer = context->peer();
        logdebug("GetGlog request index %" PRIu64 "", request->index());
    }

    string info, data;
    tie(info, data) = paxos_log_->GetInfo(request->index());
    reply->set_info(info);  
    reply->set_data(data);
    return grpc::Status::OK;
}


} // namespace glog


