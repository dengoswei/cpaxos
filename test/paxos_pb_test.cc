#include <string>
#include "paxos.pb.h"
#include "utils.h"
#include "gtest/gtest.h"


using namespace paxos;

TEST(TestLogEntry, SimpleTest)
{
    proto::LogEntry log_entry;

    RandomStrGen<10, 50> sgen;
    log_entry.set_logid(1ull);
    log_entry.set_key(sgen.Next());
    log_entry.set_value(sgen.Next());

    std::string dump_value;
    bool ret = log_entry.SerializeToString(&dump_value);
    assert(true == ret);
    assert(false == dump_value.empty());

    proto::LogEntry new_log_entry;
    ret = new_log_entry.ParseFromString(dump_value);
    assert(true == ret);
    assert(log_entry.logid() == new_log_entry.logid());
    assert(log_entry.key() == new_log_entry.key());
    assert(log_entry.value() == new_log_entry.value());
}



