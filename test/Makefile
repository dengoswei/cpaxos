
CPPFLAGS += -isystem $(GTEST_DIR)/include -std=c++14 -stdlib=libc++
CXXFLAGS += -g -Wall -Wextra



TESTS = utils_test paxos_instance_test paxos_test paxos_pb_test config_test glog_test

INCLS += -I../ -I../cpaxospb
INCLS += -I/Users/dengoswei/open-src/github.com/microsoft/GSL/include
INCLS += -I/Users/dengoswei/project/include
LINKS += -L/Users/dengoswei/project/lib
LINKS += -lpthread -lprotobuf

GTEST_HEADERS = $(GTEST_DIR)/include/gtest/*.h \
                $(GTEST_DIR)/include/gtest/internal/*.h

CPPCOMPILE = $(CXX) $(CPPFLAGS) $(CXXFLAGS) $< $(INCLS) -c -o $@
BUILDEXE = $(CXX) $(CPPFLAGS) $(CXXFLAGS) -o $@ $^ $(LINKS)

PROTOS_PATH = cpaxospb
PROTOC = /Users/dengoswei/project/bin/protoc
GRPC_CPP_PLUGIN = grpc_cpp_plugin
GRPC_CPP_PLUGIN_PATH ?= `which $(GRPC_CPP_PLUGIN)`

all: $(TESTS)

clean :
	rm -f $(TESTS) gtest.a gtest_main.a *.o ../*.o ../cpaxospb/*.o 

# Builds gtest.a and gtest_main.a.

# Usually you shouldn't tweak such internal variables, indicated by a
# trailing _.
GTEST_SRCS_ = $(GTEST_DIR)/src/*.cc $(GTEST_DIR)/src/*.h $(GTEST_HEADERS)

# For simplicity and to avoid depending on Google Test's
# implementation details, the dependencies specified below are
# conservative and not optimized.  This is fine as Google Test
# compiles fast and for ordinary users its source rarely changes.
gtest-all.o : $(GTEST_SRCS_)
	$(CXX) $(CPPFLAGS) -I$(GTEST_DIR) $(CXXFLAGS) -c \
            $(GTEST_DIR)/src/gtest-all.cc

gtest_main.o : $(GTEST_SRCS_)
	$(CXX) $(CPPFLAGS) -I$(GTEST_DIR) $(CXXFLAGS) -c \
            $(GTEST_DIR)/src/gtest_main.cc

gtest.a : gtest-all.o
	$(AR) $(ARFLAGS) $@ $^

gtest_main.a : gtest-all.o gtest_main.o
	$(AR) $(ARFLAGS) $@ $^

utils_test: utils_test.o gtest_main.a
	$(BUILDEXE)

paxos_instance_test: paxos_instance_test.o ../paxos_instance.o ../cpaxospb/paxos.pb.o gtest_main.a
	$(BUILDEXE)

paxos_test: paxos_test.o ../paxos_instance.o ../paxos_impl.o ../paxos.o ../cpaxospb/paxos.pb.o gtest_main.a
	$(BUILDEXE)

paxos_pb_test: paxos_pb_test.o ../cpaxospb/paxos.pb.o gtest_main.a
	$(BUILDEXE)

%.pb.cc: ../cpaxospb/%.proto
	$(PROTOC) -I $(PROTOS_PATH) --cpp_out=../cpaxospb/ $<


%.o:%.cc
	$(CPPCOMPILE)

#.cc.o:
#	$(CPPCOMPILE)

