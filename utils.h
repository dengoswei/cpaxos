#pragma once

#include <tuple>
#include <string>
#include <algorithm>
#include <random>
#include <chrono>
#include <limits>
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

namespace {

void log_nothing(const char* /* format */, ...) 
    __attribute__((format(printf, 1, 2)));

void log_nothing(const char* /* format */, ...) {

}

} // namespace

#ifndef TEST_DEBUG

#define logdebug(format, ...) log_nothing(format, ##__VA_ARGS__)
#define logerr(format, ...) log_nothing(format, ##__VA_ARGS__)

#else

#define logdebug(format, ...) \
    printf("[PAXOS DEBUG: %s %s %d] " format "\n", __FILE__, __func__, __LINE__, ##__VA_ARGS__)

#define logerr(format, ...) \
    printf("[PAXOS ERROR: %s %s %d] " format "\n", __FILE__, __func__, __LINE__, ##__VA_ARGS__)

#endif

namespace paxos {

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
    PropNumGen(uint8_t selfid, uint64_t prop_cnt) 
        : selfid_(selfid)
        , prop_cnt_(prop_cnt)
    {

    }

    PropNumGen(uint64_t prop_num) {
        std::tie(selfid_, prop_cnt_) = prop_num_decompose(prop_num);
        assert(0 != selfid_);
    }

    uint64_t Get() const {
        return prop_num_compose(selfid_, prop_cnt_);
    }

    bool Update(uint64_t prop_num) {
        uint8_t id = 0;
        uint64_t cnt = 0;
        std::tie(id, cnt) = prop_num_decompose(prop_num);
        if (id != selfid_ || cnt <= prop_cnt_) {
            return false;
        }

        prop_cnt_ = cnt;
        return true;
    }

    uint64_t Next(uint64_t hint_num)
    {
        uint8_t hint_id = 0;
        uint64_t hint_prop_cnt = 0;
        std::tie(hint_id, hint_prop_cnt) = prop_num_decompose(hint_num);    
        
        uint64_t prev_prop_num = 
            prop_num_compose(selfid_, prop_cnt_);
        if (prop_cnt_+1 <= hint_prop_cnt && selfid_ < hint_id) {
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

    bool IsLocalNum(uint64_t prop_num) const {
        auto id_cnt = prop_num_decompose(prop_num);
        return selfid_ == std::get<0>(id_cnt);
    }

private:
    uint8_t selfid_;
    uint64_t prop_cnt_;
};


// utils for test

template <typename RNGType,
         typename INTType,
         INTType iMin=0, INTType iMax=std::numeric_limits<INTType>::max()>
class RandomIntGen
{
public:
    RandomIntGen()
        : m_tUDist(iMin, iMax)
    {
        m_tMyRNG.seed(time(NULL));
    }

    INTType Next()
    {
        return m_tUDist(m_tMyRNG);
    }

private:
    RNGType m_tMyRNG;
    std::uniform_int_distribution<INTType> m_tUDist;
};

typedef RandomIntGen<std::mt19937_64, uint64_t> Random64BitGen;
typedef RandomIntGen<std::mt19937, uint32_t> Random32BitGen;

static const char DICTIONARY[] =
    "0123456789"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "abcdefghijklmnopqrstuvwxyz";


template <int iMin, int iMax>
class RandomStrGen
{
public:
    std::string Next()
    {
        auto iLen = m_tRLen.Next();
        std::string s;
        s.resize(iLen);
        for (auto i = 0; i < iLen; ++i)
        {
            auto j = m_tRIdx.Next();
            s[i] = DICTIONARY[j];
            assert(s[i] != '\0');
        }
        return s;
    }

private:
    RandomIntGen<std::mt19937, int, iMin, iMax> m_tRLen;
    RandomIntGen<std::mt19937, int, 0, sizeof(DICTIONARY)-2> m_tRIdx;
};

namespace measure {

template <typename F, typename ...Args>
inline std::tuple<typename std::result_of<F(Args...)>::type, std::chrono::milliseconds>
execution(F func, Args&&... args) {
    auto start = std::chrono::system_clock::now();
    auto ret = func(std::forward<Args>(args)...);
    auto duration = std::chrono::duration_cast<
        std::chrono::milliseconds>(std::chrono::system_clock::now() - start);
    return std::make_tuple(ret, duration);
}

} // namespace measure

} // namespace paxos

