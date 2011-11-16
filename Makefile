# ------------------------------------------------------------------------------
# Targets

NODE_TARGET = src/main/dhtnode.d
NODE_OUTPUT = bin/dhtnode

COPY_TARGET = src/main/dhtcopy.d
COPY_OUTPUT = bin/dhtcopy

DUMP_TARGET = src/main/dhtdump.d
DUMP_OUTPUT = bin/dhtdump

INFO_TARGET = src/main/dhtinfo.d
INFO_OUTPUT = bin/dhtinfo

CLI_TARGET = src/main/dhtcli.d
CLI_OUTPUT = bin/dhtcli

TEST_TARGET = src/main/dhttest.d
TEST_OUTPUT = bin/dhttest

PERFORMANCE_TARGET = src/main/dhtperformance.d
PERFORMANCE_OUTPUT = bin/dhtperformance

HASHRANGE_TARGET = src/main/dhthashrange.d
HASHRANGE_OUTPUT = bin/dhthashrange


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
	-L-lminilzo \
	-L-ldl \
	-I../swarm \
	-version=NewTango

UNITTESTFLAGS =\
	-unittest \
	-debug=OceanUnitTest

RELEASE_FLAGS = ${FLAGS}\
	-L-s

DEBUG_FLAGS = ${FLAGS}\
	-debug -gc ${UNITTESTFLAGS}

NODE_FLAGS =\
	-L-ltokyocabinet
#	-debug=Raw
#	-debug=SelectFiber\
#	-debug=ISelectClient\
#	-debug=ConnectionHandler
#	-debug=Raw

CLIENT_FLAGS =\
	-L-lebtree
#	-debug=ISelectClient\
#	-debug=SwarmClient
#	-debug=Raw
#	-debug=ISelectClient


# ------------------------------------------------------------------------------
# Debug build of all targets (default)

.PHONY: revision node info test cli performance

default: node info cli


# ------------------------------------------------------------------------------
# Revision file build

DEPENDENCIES = ocean swarm tango

revision:
	@../ocean/script/mkversion.sh $(D_GC) $(DEPENDENCIES)


# ------------------------------------------------------------------------------
# node debug & release builds

node: revision
	xfbuild +D=.deps-node +O=.objs-node +o=${NODE_OUTPUT} ${XFBUILD_FLAGS} ${DEBUG_FLAGS} ${NODE_FLAGS} ${NODE_TARGET}

node-release: revision
	xfbuild +D=.deps-node +O=.objs-node +o=${NODE_OUTPUT} ${XFBUILD_FLAGS} ${RELEASE_FLAGS} ${NODE_FLAGS} ${NODE_TARGET}


# ------------------------------------------------------------------------------
# copy debug & release builds

copy:
	xfbuild +D=.deps-copy +O=.objs-copy +o=${COPY_OUTPUT} ${XFBUILD_FLAGS} ${DEBUG_FLAGS} ${CLIENT_FLAGS} ${COPY_TARGET}

copy-release:
	xfbuild +D=.deps-copy +O=.objs-copy +o=${COPY_OUTPUT} ${XFBUILD_FLAGS} ${RELEASE_FLAGS} ${CLIENT_FLAGS} ${COPY_TARGET}


# ------------------------------------------------------------------------------
# dump debug & release builds

dump:
	xfbuild +D=.deps-dump +O=.objs-dump +o=${DUMP_OUTPUT} ${XFBUILD_FLAGS} ${DEBUG_FLAGS} ${CLIENT_FLAGS} ${DUMP_TARGET}

dump-release:
	xfbuild +D=.deps-dump +O=.objs-dump +o=${DUMP_OUTPUT} ${XFBUILD_FLAGS} ${RELEASE_FLAGS} ${CLIENT_FLAGS} ${DUMP_TARGET}


# ------------------------------------------------------------------------------
# info debug & release builds

info:
	xfbuild +D=.deps-info +O=.objs-info +o=${INFO_OUTPUT} ${XFBUILD_FLAGS} ${DEBUG_FLAGS} ${CLIENT_FLAGS} ${INFO_TARGET}

info-release:
	xfbuild +D=.deps-info +O=.objs-info +o=${INFO_OUTPUT} ${XFBUILD_FLAGS} ${RELEASE_FLAGS} ${CLIENT_FLAGS} ${INFO_TARGET}


# ------------------------------------------------------------------------------
# command line client debug & release builds

cli:
	xfbuild +D=.deps-$@ +O=.objs-$@ +o=${CLI_OUTPUT} ${XFBUILD_FLAGS} ${DEBUG_FLAGS} ${CLIENT_FLAGS} ${CLI_TARGET}

cli-release:
	xfbuild +D=.deps-$@+O=.objs-$@ +o=${CLI_OUTPUT} ${XFBUILD_FLAGS} ${RELEASE_FLAGS} ${CLIENT_FLAGS} ${CLI_TARGET}


# ------------------------------------------------------------------------------
# test debug & release builds

test:
	xfbuild +D=.deps-test +O=.objs-test +o=${TEST_OUTPUT} ${XFBUILD_FLAGS} ${DEBUG_FLAGS} ${CLIENT_FLAGS} ${TEST_TARGET}

test-release:
	xfbuild +D=.deps-test +O=.objs-test +o=${TEST_OUTPUT} ${XFBUILD_FLAGS} ${RELEASE_FLAGS} ${CLIENT_FLAGS} ${TEST_TARGET}


# ------------------------------------------------------------------------------
# performance debug & release builds

performance:
	xfbuild +D=.deps-performance +O=.objs-performance +o=${PERFORMANCE_OUTPUT} ${XFBUILD_FLAGS} ${DEBUG_FLAGS} ${CLIENT_FLAGS} ${PERFORMANCE_TARGET}

performance-release:
	xfbuild +D=.deps-performance +O=.objs-performance +o=${PERFORMANCE_OUTPUT} ${XFBUILD_FLAGS} ${RELEASE_FLAGS} ${CLIENT_FLAGS} ${PERFORMANCE_TARGET}


# ------------------------------------------------------------------------------
# hash range debug & release builds

hashrange:
	xfbuild +D=.deps-hashrange +O=.objs-hashrange +o=${HASHRANGE_OUTPUT} ${XFBUILD_FLAGS} ${DEBUG_FLAGS} ${CLIENT_FLAGS} ${HASHRANGE_TARGET}

hashrange-release:
	xfbuild +D=.deps-hashrange +O=.objs-hashrange +o=${HASHRANGE_OUTPUT} ${XFBUILD_FLAGS} ${RELEASE_FLAGS} ${CLIENT_FLAGS} ${HASHRANGE_TARGET}


# ------------------------------------------------------------------------------
# Upload node

EU_NODE_SERVERS = 1 2 3 4 5 6 7

US_NODE_SERVERS = 1 2 3 4 5 6

upload-node-eu:
	$(foreach srv, $(EU_NODE_SERVERS), scp -C ${NODE_OUTPUT} root@eq6-$(srv).sociomantic.com:/tmp/dht;)

upload-node-us:
	$(foreach srv, $(US_NODE_SERVERS), scp -C ${NODE_OUTPUT} root@rs-$(srv).sociomantic.com:/tmp/dht;)


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
	@-rm revisions.txt
	@-rm .objs-* -rf
	@-rm .deps-* -rf
