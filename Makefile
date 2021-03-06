
CPPFLAGS += -isystem $(GTEST_DIR)/include -std=c++11 -stdlib=libc++
CXXFLAGS += -g -Wall -Wextra -D TEST_DEBUG

TESTS = utils_test paxos_instance_test paxos_test paxos_pb_test config_test glog_test

INCLS += -I./cpaxospb/
INCLS += -I/Users/dengoswei/project/include
INCLS += -I../cutils/
LINKS += -L/Users/dengoswei/project/lib
LINKS += -lpthread -lprotobuf

AR = ar -rc
CPPCOMPILE = $(CXX) $(CPPFLAGS) $(CXXFLAGS) $< $(INCLS) -c -o $@
BUILDEXE = $(CXX) $(CPPFLAGS) $(CXXFLAGS) -o $@ $^ $(LINKS)
ARSTATICLIB = $(AR) $@ $^ $(AR_FLAGS)

PROTOS_PATH = cpaxospb
PROTOC = /Users/dengoswei/project/bin/protoc

all: $(TESTS)

clean :
	rm -f $(TESTS) *.o cpaxospb/*.o cpaxospb/paxos.pb.* test/*.o libcpaxos.a

libcpaxos.a: paxos.o paxos_impl.o paxos_instance.o cpaxospb/paxos.pb.o
	$(ARSTATICLIB)

%.pb.cc: cpaxospb/%.proto
	$(PROTOC) -I $(PROTOS_PATH) --cpp_out=cpaxospb/ $<

%.o:%.cc
	$(CPPCOMPILE)

#.cc.o:
#	$(CPPCOMPILE)

