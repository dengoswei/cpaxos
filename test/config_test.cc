#include <iostream>
#include "utils.h"
#include "gtest/gtest.h"
#include "gsl.h"
#include "config.h"
#include "rapidjson/document.h"
#include "rapidjson/filereadstream.h"


using namespace rapidjson;


using namespace std;
using namespace paxos;


TEST(TestConfig, SimpleConfig)
{
    const char* sFileName = "./config.example.json";
    gsl::cstring_view<> filename(sFileName, strlen(sFileName));

    Config config(filename);
    auto& json = config.GetDocument();
    assert(json.IsObject());

    cout << "selfid: " << json["selfid"].GetInt() << endl;
    cout << "selfid: " << config.GetSelfId() << endl;
    cout << "groups: " << endl;

    for (auto& iter : config.GetGroups()) {
        cout << " id : " << iter.first << " addr: " << iter.second << endl;
    }
    
    for (Value::ConstValueIterator iter = json["groups"].Begin(); 
            iter != json["groups"].End(); ++iter) {
        cout << "  id: " << (*iter)["id"].GetInt() 
             << " addr: " << (*iter)["addr"].GetString() << endl;
    }

}


