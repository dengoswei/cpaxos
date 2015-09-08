#pragma once

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
    uint64_t *accepted_num;
    const std::string* accepted_value;
};


// IMPORTANT: NOT thread safe
class PropNumGen {

public:
    PropNumGen(uint8_t selfid, uint64_t prop_cnt)
        : selfid_(selfid)
        , prop_cnt_(prop_cnt)
    {

    }

    uint64_t Get()
    {
        return compose(selfid_, prop_cnt_);
    }

    uint64_t Next(uint64_t hint_num)
    {
        uint8_t hint_id = 0;
        uint64_t hint_prop_cnt = 0;
        std::tie(hint_id, hint_prop_cnt) = decompose(hint_num);    
        
        uint64_t prev_prop_num = compose(selfid_, prop_cnt_);
        prop_cnt_ = max(prop_cnt_ + 1, hint_prop_cnt);
        uint64_t next_prop_num = compose(selfid_, prop_cnt_);
        assert(prev_prop_num < next_prop_num);
        return next_prop_num;
    }

private:
    uint64_t compose(uint8_t id, uint64_t prop_cnt)
    {
        return prop_cnt << 8 + id;
    }

    std::tuple<uint8_t, uint64_t> decompose(uint64_t prop_num)
    {
        // id, prop_cnt
        return make_tuple(prop_num & 0xFF, prop_num >> 8);
    }

private:
    uint8_t selfid_;
    uint64_t prop_cnt_;
};



} // namespace paxos

