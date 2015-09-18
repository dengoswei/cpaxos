
CPPFLAGS += -isystem $(GTEST_DIR)/include -std=c++11
CXXFLAGS += -g -Wall -Wextra 




INCLS += -I../capnp
LINKS += -lcapnp-rpc -lkj -lkj-async -lcapnp


CPPCOMPILE = $(CXX) $(CPPFLAGS) $(CXXFLAGS) $< $(INCLS) -c -o $@
BUILDEXE = $(CXX) $(CPPFLAGS) $(CXXFLAGS) -o $@ $^ $(LINKS)

PROGS = paxos_server paxos_client

all: $(PROGS)

clean :
	rm -f *.o ../capnp/*.o $(PROGS)


paxos_server: paxos_server.o ../capnp/simple.capnp.o
	$(BUILDEXE)

paxos_client: paxos_client.o ../capnp/simple.capnp.o
	$(BUILDEXE)

%.o:%.cc
	$(CPPCOMPILE)

%.o:%.c++
	$(CPPCOMPILE)

#.cc.o:
#	$(CPPCOMPILE)

