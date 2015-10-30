#include <unistd.h>
#include <iostream>
#include <string>
#include <memory>
#include <thread>
#include <future>
#include <map>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <vector>
#include <grpc++/grpc++.h>
#include <cstring>
#include <cassert>
#include <stdint.h>
#include "gsl.h"
#include "config.h"
#include "paxos.h"
#include "paxos.pb.h"
#include "utils.h"
#include "glog_server_impl.h"
#include "glog_client_impl.h"

using namespace std;
using namespace paxos;
using namespace glog;


class Queue {

public:
    Queue() = default;
    ~Queue() = default;

    void Push(std::unique_ptr<Message> msg)
    {
        {
            lock_guard<mutex> lock(mutex_);
            msg_.emplace(move(msg));
        }

        cv_.notify_one();
    }

    std::unique_ptr<Message> Pop()
    {
        {
            unique_lock<mutex> lock(mutex_);
            while (msg_.empty()) {
                cv_.wait(lock, [&]() {
                    return !msg_.empty();
                });
            }

            assert(false == msg_.empty());
            auto msg = move(msg_.front());
            msg_.pop();
            return msg;
        }
    }

private:
    std::mutex mutex_;
    std::condition_variable cv_;
    std::queue<std::unique_ptr<paxos::Message>> msg_;
};


int single_call(
        const std::map<uint64_t, std::string>& groups, const Message& msg) 
{
    assert(0 != msg.to_id());
    assert(0 != msg.peer_id());

    auto svrid = msg.to_id();

    glog::GlogClientImpl client(svrid, 
            grpc::CreateChannel(groups.at(svrid), grpc::InsecureCredentials()));
    client.PostMsg(msg);
    logdebug("single_call PostMsg svrid %" PRIu64 
            " msg type %d", svrid, static_cast<int>(msg.type()));
    return 0;
}

int RPCWorker(
        uint64_t selfid, 
        const std::map<uint64_t, std::string>& groups, 
        Queue& queue)
{
    while (true) {
        unique_ptr<Message> msg = queue.Pop();

        // deal with the msg;
        assert(nullptr != msg);
        assert(selfid == msg->peer_id());
        assert(0 < msg->index());

        logdebug("TEST index %" PRIu64 " msgtype %d to_id %" PRIu64 " peer_id %" PRIu64 " ", 
                msg->index(), static_cast<int>(msg->type()),
                msg->to_id(), msg->peer_id());

        int ret = 0;
        if (0 != msg->to_id()) {
            ret = single_call(groups, *msg);
        } else {
            // broadcast
            for (auto& piter : groups) {
                if (selfid == piter.first) {
                    continue;
                }

                msg->set_to_id(piter.first);
                ret = single_call(groups, *msg);
            }
        }
    }
}


class CallBack {

public:
    CallBack(uint64_t selfid, 
            const std::map<uint64_t, std::string>& groups)
        : selfid_(selfid)
        , groups_(groups)
        , queue_(make_shared<Queue>())
        , rpc_worker_(make_shared<thread>(RPCWorker, selfid_, groups_, ref(*queue_)))
    {
        assert(0 < selfid_);
        assert(groups_.end() != groups_.find(selfid_));
        assert(nullptr != queue_);
        assert(nullptr != rpc_worker_);
        rpc_worker_->detach();
    }

    int operator()(
            std::unique_ptr<HardState> hs, 
            std::unique_ptr<Message> msg)
    {
        int ret = 0;
        if (nullptr != hs) {
            assert(0 < hs->index());
            logdebug("TEST index %" PRIu64 " store hs", index);
        }

        if (nullptr != msg) {
            queue_->Push(move(msg));
        }

        logdebug("TEST index %" PRIu64 " hs %p msg %p", 
                index, hs.get(), msg.get());
        return 0;
    }

private:
    uint64_t selfid_;
    std::map<uint64_t, std::string> groups_;

    std::shared_ptr<Queue> queue_;
    std::shared_ptr<std::thread> rpc_worker_;
};


void StartServer(uint64_t selfid, const std::map<uint64_t, std::string>& groups)
{
    unique_ptr<Paxos> paxos_log = 
        unique_ptr<Paxos>{new Paxos{selfid, groups.size()}};
    assert(nullptr != paxos_log);

    CallBack callback(selfid, groups);
    GlogServiceImpl service(groups, move(paxos_log), callback);
    assert(nullptr == paxos_log);

    grpc::ServerBuilder builder;
    builder.AddListeningPort(
            groups.at(selfid), grpc::InsecureServerCredentials());
    builder.RegisterService(&service);

    unique_ptr<grpc::Server> server(builder.BuildAndStart());
    logdebug("Server %" PRIu64 " listening on %s\n", 
            selfid, groups.at(selfid).c_str());

    server->Wait();
    return ;
}

int SimplePropose(
        int index, 
        uint64_t svrid, 
        const std::map<uint64_t, std::string>& groups)
{
    GlogClientImpl client(svrid, 
            grpc::CreateChannel(groups.at(svrid), grpc::InsecureCredentials()));

    string sData("dengos@test.com");
    int ret = client.Propose({sData.data(), sData.size()});
    logdebug("try index %d client.Propose svrid %" PRIu64  " ret %d", 
            index, svrid, ret);

    {
        string info, data;

        tie(info, data) = client.GetGlog(index);
        logdebug("svrid %" PRIu64 " index %d info %s data %s", 
                svrid, index, info.c_str(), data.c_str());
    }

    {
        int retcode = 0;
        uint64_t max_index = 0;
        uint64_t commited_index = 0;
        tie(retcode, max_index, commited_index) = client.GetPaxosInfo();
        logdebug("client.GetPaxosInfo retcode %d max_index %" PRIu64 
                 " commited_index %" PRIu64, 
                 retcode, max_index, commited_index);
    }

    return ret;
}

void SimpleTryCatchUp(
        uint64_t svrid, const std::map<uint64_t, std::string>& groups)
{
    GlogClientImpl client(svrid, 
            grpc::CreateChannel(groups.at(svrid), grpc::InsecureCredentials()));
    client.TryCatchUp();
}

    int
main ( int argc, char *argv[] )
{
    const char* sFileName = "../test/config.example.json";

    Config config(gsl::cstring_view<>{sFileName, strlen(sFileName)});

    auto groups = config.GetGroups();

    vector<future<void>> vec;
    for (auto piter : groups) {
        auto res = async(launch::async, 
                StartServer, piter.first, cref(groups));
        vec.push_back(move(res));
    }

    // test
    sleep(2);
    for (int i = 0; i < 10;) {
        int ret = SimplePropose(i+1, 1ull, groups);
        if (0 == ret) {
            ++i;
            continue;
        }

        // else
        usleep(1000);
    }

    SimpleTryCatchUp(1ull, groups);

    for (auto& v : vec) {
        v.get();
    }

    return EXIT_SUCCESS;
}				/* ----------  end of function main  ---------- */

