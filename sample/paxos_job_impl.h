#pragma once


namespace paxos {



class PostJobImpl {

public:
    void submit(
            std::tuple<uint64_t, Message>&& item, 
            ::kj::Own<PromiseFulfiller<int>>&& fulfiller)
    {
        {
            lock_guard<mutex> lock(queue_mutex_);
            queue_.emplace(forward(item), forward(fulfiller));
        }
        paxos_cv_.notify_one();
    }

private:
    std::mutex queue_mutex_;
    std::condition_variable queue_cv_;
    std::queue<std::tuple<
        std::tuple<uint64_t, paxos::Message>, 
        ::kj::Own<PromiseFulfiller<int>>>> queue_;
};



class GetJobImpl {

};


class SetJobImpl {

};


} // namespace paxos


