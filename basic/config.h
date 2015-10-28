#pragma once



class Document;


namespace paxos {


class Config {

public:
    Config(const std::string_view& sConfigFileName);

    ~Config();


private:
    std::unique_ptr<Document> json_;
}; 



} // namespace paxos


