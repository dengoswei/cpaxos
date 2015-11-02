
CPPFLAGS += -isystem $(GTEST_DIR)/include -std=c++14 -stdlib=libc++
CXXFLAGS += -g -Wall -Wextra

TESTS = utils_test paxos_instance_test paxos_test paxos_pb_test config_test glog_test

INCLS += -I./cpaxospb/
INCLS += -I/Users/dengoswei/open-src/github.com/microsoft/GSL/include
INCLS += -I/Users/dengoswei/project/include
LINKS += -L/Users/dengoswei/project/lib
LINKS += -lpthread -lprotobuf

AR = ar -rc
CPPCOMPILE = $(CXX) $(CPPFLAGS) $(CXXFLAGS) $< $(INCLS) -c -o $@
BUILDEXE = $(CXX) $(CPPFLAGS) $(CXXFLAGS) -o $@ $^ $(LINKS)
ARSTATICLIB = $(AR) $@ $^ $(AR_FLAGS)

PROTOS_PATH = cpaxospb
PROTOC = /Users/dengoswei/project/bin/protoc
GRPC_CPP_PLUGIN = grpc_cpp_plugin
GRPC_CPP_PLUGIN_PATH ?= `which $(GRPC_CPP_PLUGIN)`

all: $(TESTS)

clean :
	rm -f $(TESTS) *.o cpaxospb/*.o test/*.o

libcpaxos.a: paxos.o paxos_impl.o paxos_instance.o cpaxospb/paxos.pb.o
	$(ARSTATICLIB)

%.pb.cc: cpaxospb/%.proto
	$(PROTOC) -I $(PROTOS_PATH) --cpp_out=cpaxospb/ $<

%.o:%.cc
	$(CPPCOMPILE)

#.cc.o:
#	$(CPPCOMPILE)

