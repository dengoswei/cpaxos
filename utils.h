#pragma once

#include <tuple>
#include <algorithm>
#include <cassert>
#include <stdint.h>

namespace paxos {

//inline uint64_t prop_num_compose(uint8_t id, uint64_t prop_cnt)
//{
//    return (prop_cnt << 8) + id;
//}
//
//inline 
//std::tuple<uint8_t, uint64_t> prop_num_decompose(uint64_t prop_num)
//{
//    return std::make_tuple(prop_num & 0xFF, prop_num >> 8);
//}
//
//// IMPORTANT: NOT thread safe
//class PropNumGen {
//
//public:
//    PropNumGen(uint8_t selfid, uint64_t prop_cnt) 
//        : selfid_(selfid)
//        , prop_cnt_(prop_cnt)
//    {
//
//    }
//
//    PropNumGen(uint64_t prop_num) {
//        std::tie(selfid_, prop_cnt_) = prop_num_decompose(prop_num);
//        assert(0 != selfid_);
//    }
//
//    uint64_t Get() const {
//        return prop_num_compose(selfid_, prop_cnt_);
//    }
//
//    // after update: Get() >= prop_num
//    bool Update(uint64_t prop_num) 
//    {
//        if (Get() >= prop_num) {
//            return false;
//        }
//
//        // else assert(Get() < prop_num);
//        uint8_t id = 0;
//        uint64_t cnt = 0ull;
//        std::tie(id, cnt) = prop_num_decompose(prop_num);
//        if (id > selfid_) {
//            prop_cnt_ = cnt + 1ull;
//        }
//        else {
//            // assert(id <= selfid_);
//            prop_cnt_ = cnt;
//        }
//
//        assert(Get() >= prop_num);
//        return true;
//    }
//
//    // after update: Get() > hint_num
//    uint64_t Next(uint64_t hint_num) 
//    {
//        const auto old_prop_cnt = prop_cnt_;
//        Update(hint_num);
//        assert(old_prop_cnt <= prop_cnt_);
//        if (old_prop_cnt == prop_cnt_) {
//            ++prop_cnt_;
//        }
//
//        return Get(); 
//    }
//
//    bool IsLocalNum(uint64_t prop_num) const {
//        auto id_cnt = prop_num_decompose(prop_num);
//        return selfid_ == std::get<0>(id_cnt);
//    }
//
//private:
//    uint8_t selfid_;
//    uint64_t prop_cnt_;
//};

} // namespace paxos

