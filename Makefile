# ------------------------------------------------------------------------------
# Targets

NODE_TARGET = src/main/dhtnode.d
NODE_OUTPUT = bin/dhtnode

REMOVE_TARGET = src/main/dhtremove.d
REMOVE_OUTPUT = bin/dhtremove

INFO_TARGET = src/main/dhtinfo.d
INFO_OUTPUT = bin/dhtinfo

TEST_TARGET = src/main/dhttest.d
TEST_OUTPUT = bin/dhttest


# ------------------------------------------------------------------------------
# Xfbuild flags

XFBUILD_FLAGS =\
	+c=dmd


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
# -debug=ConnectionHandler -debug=Raw

NODE_FLAGS =\
	-L-ltokyocabinet

CLIENT_FLAGS =\
	-L-lebtree


# ------------------------------------------------------------------------------
# Debug build of all targets (default)

default: node remove info test


# ------------------------------------------------------------------------------
# node debug & release builds

node:
	xfbuild +D=.deps-node +O=.objs-node +o=${NODE_OUTPUT} ${XFBUILD_FLAGS} ${DEBUG_FLAGS} ${NODE_FLAGS} ${NODE_TARGET}

node-release:
	xfbuild +D=.deps-node +O=.objs-node +o=${NODE_OUTPUT} ${XFBUILD_FLAGS} ${RELEASE_FLAGS} ${NODE_FLAGS} ${NODE_TARGET}



# ------------------------------------------------------------------------------
# remove debug & release builds

remove:
	xfbuild +D=.deps-remove +O=.objs-remove +o=${REMOVE_OUTPUT} ${XFBUILD_FLAGS} ${DEBUG_FLAGS} ${CLIENT_FLAGS} ${REMOVE_TARGET}

remove-release:
	xfbuild +D=.deps-remove +O=.objs-remove +o=${REMOVE_OUTPUT} ${XFBUILD_FLAGS} ${RELEASE_FLAGS} ${CLIENT_FLAGS} ${REMOVE_TARGET}


# ------------------------------------------------------------------------------
# info debug & release builds

info:
	xfbuild +D=.deps-info +O=.objs-info +o=${INFO_OUTPUT} ${XFBUILD_FLAGS} ${DEBUG_FLAGS} ${CLIENT_FLAGS} ${INFO_TARGET}

info-release:
	xfbuild +D=.deps-info +O=.objs-info +o=${INFO_OUTPUT} ${XFBUILD_FLAGS} ${RELEASE_FLAGS} ${CLIENT_FLAGS} ${INFO_TARGET}


# ------------------------------------------------------------------------------
# test debug & release builds

test:
	xfbuild +D=.deps-test +O=.objs-test +o=${TEST_OUTPUT} ${XFBUILD_FLAGS} ${DEBUG_FLAGS} ${CLIENT_FLAGS} ${TEST_TARGET}

test-release:
	xfbuild +D=.deps-test +O=.objs-test +o=${TEST_OUTPUT} ${XFBUILD_FLAGS} ${RELEASE_FLAGS} ${CLIENT_FLAGS} ${TEST_TARGET}


# ------------------------------------------------------------------------------
# Upload

EU_SERVERS = 1 2 3 4 5 6 7

upload-node-eu:
	$(foreach srv, $(EU_SERVERS), scp -C ${NODE_OUTPUT} root@eq6-$(srv).sociomantic.com:/tmp/dht;)


# ------------------------------------------------------------------------------
# Cleanup

clean:
	xfbuild ${XFBUILD_FLAGS} +clean ${NODE_TARGET}
	xfbuild ${XFBUILD_FLAGS} +clean ${REMOVE_TARGET}
	xfbuild ${XFBUILD_FLAGS} +clean ${INFO_TARGET}
	xfbuild ${XFBUILD_FLAGS} +clean ${TEST_TARGET}
	@-rm .objs-* -rf
	@-rm .deps-* -rf
