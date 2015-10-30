
#include "glog_client_impl.h"
#include "utils.h"
#include "glog.pb.h"
#include "paxos.pb.h"


using grpc::ClientContext;
using grpc::Status;

namespace glog {

GlogClientImpl::GlogClientImpl(
        const uint64_t svrid, 
        std::shared_ptr<grpc::Channel> channel)
    : svrid_(svrid)
    , stub_(glog::Glog::NewStub(channel))
{
    assert(0 < svrid_);
}

GlogClientImpl::~GlogClientImpl() = default;

void GlogClientImpl::PostMsg(const paxos::Message& msg)
{
    NoopMsg reply;

    ClientContext context;
    hassert(msg.to_id() == svrid_, "msg.peer_id %" PRIu64 
            " svrid_ %" PRIu64 "", msg.peer_id(), svrid_);
    Status status = stub_->PostMsg(&context, msg, &reply);
    if (status.ok()) {
        logdebug("index %" PRIu64 " PostMsg succ", msg.index());
    } else {
        logerr("index %" PRIu64 " PostMsg failed", msg.index());
    }

    return ;
}

int GlogClientImpl::Propose(gsl::cstring_view<> data)
{
    ProposeRequest request;
    request.set_data(data.data(), data.size());
    
    ProposeResponse reply;

    ClientContext context;
    Status status = stub_->Propose(&context, request, &reply);
    if (status.ok()) {
        return reply.retcode();
    }

    auto error_message = status.error_message();
    logerr("Propose failed error_code %d error_message %s", 
            static_cast<int>(status.error_code()), error_message.c_str());
    return static_cast<int>(status.error_code());
}

std::tuple<std::string, std::string> GlogClientImpl::GetGlog(uint64_t index)
{
    assert(0 < index);
    GetGlogRequest request;
    request.set_index(index);

    GetGlogResponse reply;

    ClientContext context; 
    Status status = stub_->GetGlog(&context, request, &reply);
    if (status.ok()) {
        return make_tuple(reply.info(), reply.data());
    }

    auto error_message = status.error_message();
    logerr("Propose failed error_code %d error_message %s", 
            static_cast<int>(status.error_code()), error_message.c_str());
    return make_tuple(move(error_message), "");
}

GlogAsyncClientImpl::GlogAsyncClientImpl(
        const uint64_t selfid, 
        std::shared_ptr<grpc::Channel> channel)
    : selfid_(selfid)
    , stub_(glog::Glog::NewStub(channel))
{

}

GlogAsyncClientImpl::~GlogAsyncClientImpl() = default;

void GlogAsyncClientImpl::gc()
{
    const size_t LOW_WATER_MARK = 0;
    void* got_tag = nullptr;
    bool ok = false;
    while (rsp_map_.size() > LOW_WATER_MARK) {

        cq_.Next(&got_tag, &ok);
        if (ok) {
            auto seq = reinterpret_cast<uint64_t>(got_tag);
            assert(rsp_map_.end() != rsp_map_.find(seq));

            auto& t = rsp_map_[seq];
            if (!std::get<1>(t).ok()) {
                auto err_msg = std::get<1>(t).error_message();
                logerr("rsp seq %" PRIu64 " error msg %s", seq, err_msg.c_str());
            }
            rsp_map_.erase(seq); // success gc 1
        }
    }
}

void GlogAsyncClientImpl::PostMsg(const paxos::Message& msg)
{
    ClientContext context;

    uint64_t seq = ++rpc_seq_;
    auto& t = rsp_map_[seq];
    std::get<0>(t) = stub_->AsyncPostMsg(&context, msg, &cq_);
    std::get<0>(t)->Finish(&std::get<2>(t), &std::get<1>(t), reinterpret_cast<void*>(seq));
    // DO NOT CALL cq_.Next(...); // block wait
    return ;
}


} // namespace glog



