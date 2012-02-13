# ------------------------------------------------------------------------------
# Architecture to build for (32 or 64)

# default
ARCH := 32


# ------------------------------------------------------------------------------
# Targets

NODE_TARGET = src/main/dhtnode.d
NODE_OUTPUT = bin/dhtnode-${ARCH}

COPY_TARGET = src/main/dhtcopy.d
COPY_OUTPUT = bin/dhtcopy-${ARCH}

DUMP_TARGET = src/main/dhtdump.d
DUMP_OUTPUT = bin/dhtdump-${ARCH}

INFO_TARGET = src/main/dhtinfo.d
INFO_OUTPUT = bin/dhtinfo-${ARCH}

CLI_TARGET = src/main/dhtcli.d
CLI_OUTPUT = bin/dhtcli-${ARCH}

TEST_TARGET = src/main/dhttest.d
TEST_OUTPUT = bin/dhttest-${ARCH}

PERFORMANCE_TARGET = src/main/dhtperformance.d
PERFORMANCE_OUTPUT = bin/dhtperformance-${ARCH}

HASHRANGE_TARGET = src/main/dhthashrange.d
HASHRANGE_OUTPUT = bin/dhthashrange-${ARCH}

TCM_SPLIT_TARGET = src/main/tcmsplit.d
TCM_SPLIT_OUTPUT = bin/tcmsplit-${ARCH}

REDISTRIBUTE = script/redistribute
REDISTRIBUTE_CONF = doc/redistributerc.full doc/redistributerc.local
REDISTRIBUTE_ALL = $(TCM_SPLIT_OUTPUT) $(REDISTRIBUTE) $(REDISTRIBUTE_CONF)


# ------------------------------------------------------------------------------
# Dependencies

DEPS_PATH := ..
DEPS := tango ocean swarm


# ------------------------------------------------------------------------------
# Xfbuild flags

XFBUILD_FLAGS =\
	+c=dmd


# ------------------------------------------------------------------------------
# GC to use (export is needed!)

export D_GC := basic


# ------------------------------------------------------------------------------
# dmd flags

FLAGS =\
	$(foreach d,$(DEPS),-I$(DEPS_PATH)/$d) \
	-L-lminilzo \
	-L-ldl \
	-m${ARCH}

UNITTESTFLAGS =\
	-unittest \
	-debug=OceanUnitTest

RELEASE_FLAGS = ${FLAGS}\
	-L-s

DEBUG_FLAGS = ${FLAGS}\
	-debug -gc ${UNITTESTFLAGS}

NODE_FLAGS =\
	-L-ltokyocabinet
#	-debug=ConnectionHandler\
#	-debug=Raw\
#	-debug=ISelectClient\
#	-debug=SelectFiber\

CLIENT_FLAGS =\
	-L-lebtree 
#	-debug=Raw\
#	-debug=ISelectClient\
#	-debug=SwarmClient\
#	-debug=ISelectClient\

TCM_SPLIT_FLAGS =\
    -L-lglib-2.0


# ------------------------------------------------------------------------------
# Debug build of all targets (default)

.PHONY: revision node info test cli performance

default: node info cli
all: default

# ------------------------------------------------------------------------------
# Revision file build

revision:
	@../ocean/script/mkversion.sh $(D_GC) $(DEPS)


# ------------------------------------------------------------------------------
# node debug & release builds

node: revision
	xfbuild +D=.deps-$@-${ARCH}.${BUILDMODE} +O=.objs-$@-${ARCH}.${BUILDMODE} +o=${NODE_OUTPUT} ${XFBUILD_FLAGS} ${DEBUG_FLAGS} ${NODE_FLAGS} ${NODE_TARGET}

node-release: revision
	xfbuild +D=.deps-$@-${ARCH} +O=.objs-$@-${ARCH} +o=${NODE_OUTPUT} ${XFBUILD_FLAGS} ${RELEASE_FLAGS} ${NODE_FLAGS} ${NODE_TARGET}

# ------------------------------------------------------------------------------
# copy debug & release builds

copy:
	xfbuild +D=.deps-$@-${ARCH} +O=.objs-$@-${ARCH} +o=${COPY_OUTPUT} ${XFBUILD_FLAGS} ${DEBUG_FLAGS} ${CLIENT_FLAGS} ${COPY_TARGET}

copy-release:
	xfbuild +D=.deps-$@-${ARCH} +O=.objs-$@-${ARCH} +o=${COPY_OUTPUT} ${XFBUILD_FLAGS} ${RELEASE_FLAGS} ${CLIENT_FLAGS} ${COPY_TARGET}


# ------------------------------------------------------------------------------
# dump debug & release builds

dump:
	xfbuild +D=.deps-$@-${ARCH} +O=.objs-$@-${ARCH} +o=${DUMP_OUTPUT} ${XFBUILD_FLAGS} ${DEBUG_FLAGS} ${CLIENT_FLAGS} ${DUMP_TARGET}

dump-release:
	xfbuild +D=.deps-$@-${ARCH} +O=.objs-$@-${ARCH} +o=${DUMP_OUTPUT} ${XFBUILD_FLAGS} ${RELEASE_FLAGS} ${CLIENT_FLAGS} ${DUMP_TARGET}


# ------------------------------------------------------------------------------
# info debug & release builds

info:
	xfbuild +D=.deps-$@-${ARCH} +O=.objs-$@-${ARCH} +o=${INFO_OUTPUT} ${XFBUILD_FLAGS} ${DEBUG_FLAGS} ${CLIENT_FLAGS} ${INFO_TARGET}

info-release:
	xfbuild +D=.deps-$@-${ARCH} +O=.objs-$@-${ARCH} +o=${INFO_OUTPUT} ${XFBUILD_FLAGS} ${RELEASE_FLAGS} ${CLIENT_FLAGS} ${INFO_TARGET}


# ------------------------------------------------------------------------------
# command line client debug & release builds

cli:
	xfbuild +D=.deps-$@-${ARCH} +O=.objs-$@-${ARCH} +o=${CLI_OUTPUT} ${XFBUILD_FLAGS} ${DEBUG_FLAGS} ${CLIENT_FLAGS} ${CLI_TARGET}

cli-release:
	xfbuild +D=.deps-$@-${ARCH} +o=${CLI_OUTPUT} ${XFBUILD_FLAGS} ${RELEASE_FLAGS} ${CLIENT_FLAGS} ${CLI_TARGET}


# ------------------------------------------------------------------------------
# test debug & release builds

test:
	xfbuild +D=.deps-$@-${ARCH} +O=.objs-$@-${ARCH} +o=${TEST_OUTPUT} ${XFBUILD_FLAGS} ${DEBUG_FLAGS} ${CLIENT_FLAGS} ${TEST_TARGET}

test-release:
	xfbuild +D=.deps-$@-${ARCH} +O=.objs-$@-${ARCH} +o=${TEST_OUTPUT} ${XFBUILD_FLAGS} ${RELEASE_FLAGS} ${CLIENT_FLAGS} ${TEST_TARGET}


# ------------------------------------------------------------------------------
# performance debug & release builds

performance:
	xfbuild +D=.deps-$@-${ARCH} +O=.objs-$@-${ARCH} +o=${PERFORMANCE_OUTPUT} ${XFBUILD_FLAGS} ${DEBUG_FLAGS} ${CLIENT_FLAGS} ${PERFORMANCE_TARGET}

performance-release:
	xfbuild +D=.deps-$@-${ARCH} +O=.objs-$@-${ARCH} +o=${PERFORMANCE_OUTPUT} ${XFBUILD_FLAGS} ${RELEASE_FLAGS} ${CLIENT_FLAGS} ${PERFORMANCE_TARGET}


# ------------------------------------------------------------------------------
# hash range debug & release builds

hashrange:
	xfbuild +D=.deps-$@-${ARCH} +O=.objs-$@-${ARCH} +o=${HASHRANGE_OUTPUT} ${XFBUILD_FLAGS} ${DEBUG_FLAGS} ${CLIENT_FLAGS} ${HASHRANGE_TARGET}

hashrange-release:
	xfbuild +D=.deps-$@-${ARCH} +O=.objs-$@-${ARCH} +o=${HASHRANGE_OUTPUT} ${XFBUILD_FLAGS} ${RELEASE_FLAGS} ${CLIENT_FLAGS} ${HASHRANGE_TARGET}


# ------------------------------------------------------------------------------
# tcm split debug & release builds

tcmsplit: revision
	xfbuild +D=.deps-$@-${ARCH} +O=.objs-$@-${ARCH} +o=${TCM_SPLIT_OUTPUT} ${XFBUILD_FLAGS} ${TCM_SPLIT_FLAGS} ${DEBUG_FLAGS} ${TCM_SPLIT_TARGET}

tcmsplit-release: revision
	xfbuild +D=.deps-$@-${ARCH} +O=.objs-$@-${ARCH} +o=${TCM_SPLIT_OUTPUT} ${XFBUILD_FLAGS} ${TCM_SPLIT_FLAGS} ${RELEASE_FLAGS} ${TCM_SPLIT_TARGET}


# ------------------------------------------------------------------------------
# Upload node

EU_NODE_SERVERS = 1 2 3 4 5 6 7

US_NODE_SERVERS = 1 2 3 4 5 6

upload-redistribute-eu:
	$(foreach srv, $(EU_NODE_SERVERS), scp -C ${REDISTRIBUTE_ALL} root@eq6-$(srv).sociomantic.com:/tmp/dht;)

upload-node-eu:
	$(foreach srv, $(EU_NODE_SERVERS), scp -C ${NODE_OUTPUT} root@eq6-$(srv).sociomantic.com:/tmp/dht;)

connect-eu:
	@../ocean/script/tmuxconnect.sh eu_dht_servers eu ${EU_NODE_SERVERS}

connect-memory-eu:
	@../ocean/script/tmuxconnectscreen.sh eu_memory eu memory ${EU_NODE_SERVERS}

connect-tracking-eu:
	@../ocean/script/tmuxconnectscreen.sh eu_tracking eu tracking ${EU_NODE_SERVERS}

upload-node-us:
	$(foreach srv, $(US_NODE_SERVERS), scp -C ${NODE_OUTPUT} root@rs-$(srv).sociomantic.com:/tmp/dht;)

connect-us:
	@../ocean/script/tmuxconnect.sh us_dht_servers us ${US_NODE_SERVERS}

connect-dht-us:
	@../ocean/script/tmuxconnectscreen.sh us_memory us dht ${US_NODE_SERVERS}


# ------------------------------------------------------------------------------
# Upload Command line client

EU_CLI_SERVERS = 10

upload-cli-eu:
	$(foreach srv, $(EU_CLI_SERVERS), scp -C ${CLI_OUTPUT} root@eq6-$(srv).sociomantic.com:/tmp/dht;)


# ------------------------------------------------------------------------------
# Cleanup

clean:
	xfbuild ${XFBUILD_FLAGS} +clean ${NODE_TARGET}
	xfbuild ${XFBUILD_FLAGS} +clean ${COPY_TARGET}
	xfbuild ${XFBUILD_FLAGS} +clean ${DUMP_TARGET}
	xfbuild ${XFBUILD_FLAGS} +clean ${INFO_TARGET}
	xfbuild ${XFBUILD_FLAGS} +clean ${CLI_TARGET}
	xfbuild ${XFBUILD_FLAGS} +clean ${TEST_TARGET}
	xfbuild ${XFBUILD_FLAGS} +clean ${PERFORMANCE_TARGET}
	@-rm .objs-* -rf
	@-rm .deps-* -rf
