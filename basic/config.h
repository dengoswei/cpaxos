#pragma once

#include <map>
#include <string>
#include <memory>
#include <stdint.h>
#include "gsl.h"
#include "rapidjson/document.h"


namespace paxos {


class Config {

public:
    Config(const gsl::cstring_view<>& sConfigFileName);

    ~Config();

    uint64_t GetSelfId();

    std::map<uint64_t, std::string> GetGroups();

    rapidjson::Document& GetDocument() {
        assert(nullptr != json_);
        return *(json_.get());
    }

private:
    std::unique_ptr<rapidjson::Document> json_;
}; 



} // namespace paxos


