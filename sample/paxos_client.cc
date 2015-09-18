#include <cstdio>
#include <cstdlib>
#include <cassert>
#include <capnp/ez-rpc.h>
#include <kj/debug.h>
#include "simple.capnp.h"


using namespace std;

    int
main ( int argc, char *argv[] )
{
    using paxos::capnp::simple::Simple;

    assert(2 == argc);
    capnp::EzRpcClient client(argv[1]);

    Simple::Client simple_paxos = client.getMain<Simple>();
    auto& waitScope = client.getWaitScope();

    {
        auto req = simple_paxos.setRequest();
        req.setKey("test-paxos");
        req.setValue(nullptr);

        auto setPromise = req.send();
        auto rsp = setPromise.wait(waitScope);
        printf ( "rsp %d\n", rsp.getRetcode() );
    }

    return EXIT_SUCCESS;
}				/* ----------  end of function main  ---------- */
