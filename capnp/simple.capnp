
@0xbc64ae32edc35f4b;

using Cxx = import "/capnp/c++.capnp";
$Cxx.namespace("paxos::capnp::simple");

struct LogEntry {
    id @0 : UInt64;
    key @1 : Text;
    value @2 : Data;
}


interface Simple {
    set @0 (key : Text, value : Data) -> (retcode : Int32);
    get @1 (key : Text) -> (value : Data);
}


