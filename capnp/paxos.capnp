
# unique file id, generate by `capnp id`
@0xdbaa27af48e617b9;

using Cxx = import "/capnp/c++.capnp";
$Cxx.namespace("paxos::capnp");

enum MessageType {
}

struct Message {
    type @0 : MessageType;
    index @1 : UInt64;
    proposedNum @2 : UInt64;
    promisedNum @3 : UInt64;
    acceptedNum @4 : UInt64;
    acceptedValue @5 : Data;
}


interface PaxosRPC {
    post @0 (msg : Message) -> (null : Void);
}

