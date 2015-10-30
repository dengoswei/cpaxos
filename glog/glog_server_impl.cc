#include "glog_server_impl.h"
#include "glog_client_impl.h"
#include "paxos.h"
#include "paxos.pb.h"
#include "utils.h"


using namespace std;

namespace {

using namespace glog;

std::tuple<bool, uint64_t, uint64_t>
    FindMostUpdatePeer(
            uint64_t selfid, 
            uint64_t self_commited_index, 
            const std::map<uint64_t, std::string>& groups)
{
    uint64_t peer_id = 0;
    uint64_t peer_commited_index = 0;
    // rand-shuffle the groups ?
    for (auto& piter : groups) {
        if (piter.first == selfid) {
            continue;
        }

        GlogClientImpl client(piter.first, grpc::CreateChannel(
                    piter.second, grpc::InsecureCredentials()));
        int retcode = 0;
        uint64_t max_index = 0;
        uint64_t commited_index = 0;
        tie(retcode, max_index, commited_index) = client.GetPaxosInfo();
        if (0 == retcode) {
            if (commited_index > peer_commited_index) {
                peer_id = piter.first;
                peer_commited_index = commited_index;
            }
        }
    }

    return make_tuple(
            peer_commited_index <= self_commited_index, peer_id, peer_commited_index);
}

} // namespace

namespace glog {

GlogServiceImpl::GlogServiceImpl(
        const std::map<uint64_t, std::string>& groups, 
        std::unique_ptr<paxos::Paxos>&& paxos_log, 
        paxos::Callback callback)
    : groups_(groups)
    , paxos_log_(move(paxos_log))
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
GlogServiceImpl::GetPaxosInfo(
        grpc::ServerContext* context, 
        const glog::NoopMsg* request, 
        glog::PaxosInfo* reply)
{
    assert(nullptr != context);
    assert(nullptr != request);
    assert(nullptr != reply);

    {
        string peer = context->peer();
        logdebug("peer %s", peer.c_str());
    }

    uint64_t selfid = 0;
    uint64_t max_index = 0;
    uint64_t commited_index = 0;
    tie(selfid, max_index, commited_index) = paxos_log_->GetPaxosInfo();
    reply->set_max_index(max_index);
    reply->set_commited_index(commited_index);
    return grpc::Status::OK;
}

grpc::Status
GlogServiceImpl::TryCatchUp(
        grpc::ServerContext* context, 
        const glog::NoopMsg* request, 
        glog::NoopMsg* reply)
{
    assert(nullptr != context);
    assert(nullptr != request);
    assert(nullptr != reply);

    {
        string peer = context->peer();
        logdebug("peer %s", peer.c_str());
    }

    uint64_t selfid = 0;
    uint64_t max_index = 0;
    uint64_t commited_index = 0;
    tie(selfid, max_index, commited_index) = paxos_log_->GetPaxosInfo();
    
    bool most_recent = false;
    uint64_t peer_id = 0;
    uint64_t peer_commited_index = 0;
    tie(most_recent, peer_id, peer_commited_index) = 
        FindMostUpdatePeer(selfid, commited_index, groups_);

    if (false == most_recent) {
        GlogClientImpl client(peer_id, grpc::CreateChannel(
                    groups_.at(peer_id), grpc::InsecureCredentials()));
        for (uint64_t catchup_index = commited_index + 1; 
                catchup_index <= peer_commited_index; ++catchup_index) {
            
            paxos::Message msg;
            msg.set_type(paxos::MessageType::CATCHUP);
            msg.set_peer_id(selfid);
            msg.set_to_id(peer_id);
            msg.set_index(catchup_index);

            client.PostMsg(msg);
        }
    }

    logdebug("most_recent %d seldid %" PRIu64 
            " commited_index %" PRIu64 " peer_commited_index %" PRIu64, 
            most_recent, selfid, commited_index, peer_commited_index);
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
        logdebug("GetGlog request index %" PRIu64 " %s", 
                request->index(), peer.c_str());
    }

    string info, data;
    tie(info, data) = paxos_log_->GetInfo(request->index());
    reply->set_info(info);  
    reply->set_data(data);
    return grpc::Status::OK;
}


} // namespace glog


