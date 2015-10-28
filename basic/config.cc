#include "config.h"


namespace paxos {


Config::Config(const std::string_view& sConfigFileName)
{
    ifstream fin(sConfigFileName.data());
    
    stringstream ss;
    while (fin) {
        fin >> ss;
    }

    json_ = unique_ptr<Document>{new Document};

    string json = ss.str();
    json_.Parse(json);
}

Config::~Config() = default;





} // namespace paxos


