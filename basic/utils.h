#pragma once

#include <tuple>
#include <string>
#include <algorithm>
#include <cstdio>
#include <cassert>
#include <stdint.h>
#include <inttypes.h>


#define hassert(cond, fmt, ...)                     \
{                                                   \
    bool bCond = cond;                              \
    if (!bCond)                                     \
    {                                               \
        printf ( fmt "\n", ##__VA_ARGS__ );         \
    }                                               \
    assert(bCond);                                  \
}

namespace paxos {

enum class MessageType : uint32_t {
    UNKOWN = 0, 
    NOOP, 
    PROP, 
    PROP_RSP, 
    ACCPT, 
    ACCPT_RSP, 

    CHOSEN, 
};

struct Message {
    Message() = default;

    MessageType type;
    uint64_t prop_num;
    uint64_t peer_id;
    uint64_t promised_num;
    uint64_t accepted_num;
    const std::string* accepted_value;
};


inline uint64_t prop_num_compose(uint8_t id, uint64_t prop_cnt)
{
    return (prop_cnt << 8) + id;
}

inline 
std::tuple<uint8_t, uint64_t> prop_num_decompose(uint64_t prop_num)
{
    return std::make_tuple(prop_num & 0xFF, prop_num >> 8);
}

// IMPORTANT: NOT thread safe
class PropNumGen {

public:
    PropNumGen(uint64_t prop_num)
    {
        std::tie(selfid_, prop_cnt_) = prop_num_decompose(prop_num);
        assert(0 != selfid_);
    }

    uint64_t Get()
    {
        return prop_num_compose(selfid_, prop_cnt_);
    }

    uint64_t Next(uint64_t hint_num)
    {
        uint8_t hint_id = 0;
        uint64_t hint_prop_cnt = 0;
        std::tie(hint_id, hint_prop_cnt) =
            prop_num_decompose(hint_num);    
        
        uint64_t prev_prop_num = 
            prop_num_compose(selfid_, prop_cnt_);
        if (prop_cnt_+1 <= hint_prop_cnt && selfid_ < hint_id)
        {
            ++hint_prop_cnt;
        }

        ++prop_cnt_;
        prop_cnt_ = std::max(prop_cnt_, hint_prop_cnt);
        uint64_t next_prop_num = 
            prop_num_compose(selfid_, prop_cnt_);
        hassert(prev_prop_num < next_prop_num, 
                "%" PRIu64 " %" PRIu64, prev_prop_num, next_prop_num);
        hassert(next_prop_num >= hint_num, 
                "%" PRIu64 " %" PRIu64, next_prop_num, hint_num);
        return next_prop_num;
    }

private:
    uint8_t selfid_;
    uint64_t prop_cnt_;
};



} // namespace paxos

