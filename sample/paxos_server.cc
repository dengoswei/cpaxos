

#include <utility>
#include <cstdlib>
#include <cstdio>
#include <cassert>
#include <kj/debug.h>
#include <capnp/ez-rpc.h>
#include <capnp/message.h>
#include "paxos.capnp.h"
#include "simple.capnp.h"


using namespace std;
using namespace paxos::capnp::simple;

::kj::Promise<int> setImpl(
        capnp::Text::Reader key, 
        capnp::Data::Reader value)
{
    printf ( "setImpl key %s valuelen %d\n", 
            key.cStr(), static_cast<int>(value.size()) );
    // TODO
    return -1;
}

::kj::Promise<capnp::Data::Builder> getImpl(
        capnp::Text::Reader key)
{
    printf ( "getImpl key %s\n", key.cStr() );
    return capnp::Data::Builder{};
    // TODO
    // return kj::heap<capnp::Data>(capnp::Data{});
}


class SimpleImpl final : public Simple::Server {

public:
    ::kj::Promise<void> set(SetContext context) override {
        auto params = context.getParams();
        return setImpl(params.getKey(), params.getValue()).
            then([context](int retcode) mutable {

            context.getResults().setRetcode(retcode);
        });
    }

    ::kj::Promise<void> get(GetContext context) override {
        return getImpl(context.getParams().getKey()).
            then([context](capnp::Data::Reader&& value) mutable {

            context.getResults().setValue(value);
        });
    }
};


    int
main ( int argc, char *argv[] )
{
    assert(2 == argc);

    capnp::EzRpcServer server(kj::heap<SimpleImpl>(), argv[1]);

    auto& waitScope = server.getWaitScope();
    uint32_t port = server.getPort().wait(waitScope); 
    printf ( "port %u\n", port );

    kj::NEVER_DONE.wait(waitScope);
    return EXIT_SUCCESS;
}				/* ----------  end of function main  ---------- */
