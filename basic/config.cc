#include <string>
#include <cstdlib>
#include <cassert>
#include "config.h"
#include "rapidjson/filereadstream.h"



using rapidjson::Document;
using rapidjson::FileReadStream;
using rapidjson::Value;

namespace paxos {


Config::Config(const gsl::cstring_view<>& sConfigFileName)
{
    FILE* fp = fopen(sConfigFileName.data(), "r");
    assert(nullptr != fp);

    {
        std::string sReadBuffer;
        sReadBuffer.resize(65536);
        FileReadStream is(fp, &sReadBuffer[0], sReadBuffer.size());

        json_ = std::unique_ptr<Document>{new Document};
        json_->ParseStream(is);
    }
    fclose(fp);
}

Config::~Config() = default;


uint64_t Config::GetSelfId() 
{
    assert(nullptr != json_);
    assert(true == (*json_)["selfid"].IsInt());
    return (*json_)["selfid"].GetInt();
}


std::map<uint64_t, std::string> Config::GetGroups()
{
    assert(nullptr != json_);
    assert(true == (*json_)["groups"].IsArray());

    std::map<uint64_t, std::string> groups;
    for (Value::ConstValueIterator iter = (*json_)["groups"].Begin(); 
            iter != (*json_)["groups"].End(); ++iter) {
        auto& obj = *iter;
        assert(true == obj["id"].IsInt());
        assert(true == obj["addr"].IsString());
        assert(groups.end() == groups.find(obj["id"].GetInt()));

        groups[obj["id"].GetInt()] = obj["addr"].GetString();
    }

    return groups;
}

} // namespace paxos


