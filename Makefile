# ------------------------------------------------------------------------------
# Architecture to build for (32 or 64)

# default
ARCH := 64


# ------------------------------------------------------------------------------
# Targets

NODE_TARGET = src/main/dhtnode.d
NODE_OUTPUT = bin/dhtnode-${ARCH}


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

export D_GC := cdgc


# ------------------------------------------------------------------------------
# dmd flags

FLAGS =\
	$(foreach d,$(DEPS),-I$(DEPS_PATH)/$d) \
	-L-lminilzo \
	-m${ARCH}

UNITTESTFLAGS =\
	-unittest \
	-debug=OceanUnitTest

RELEASE_FLAGS = ${FLAGS}\
	-L-s

DEBUG_FLAGS = ${FLAGS}\
	-debug -gc
#${UNITTESTFLAGS}

# FIXME: unittests disabled due to an unknown compiler bug which manifested in
# the unittest of ocean.core.ObjectPool

# TODO: tokyocabinet should only be linked with the memory node
NODE_FLAGS =\
	-L-ltokyocabinet\
    -version=CDGC
#    -debug=ConnectionHandler
#   -debug=Raw
#	-debug=ISelectClient\
#	-debug=SelectFiber\


# ------------------------------------------------------------------------------
# Debug build of all targets (default)

.PHONY: revision node

default: node
all: default


# ------------------------------------------------------------------------------
# Revision file build

revision:
	@../ocean/script/mkversion.sh -t \
	$(DEPS_PATH)/ocean/script/appVersion.d.tpl $(D_GC) $(DEPS)


# ------------------------------------------------------------------------------
# node debug & release builds

node: revision
	xfbuild +D=.deps-$@-${ARCH} +O=.objs-$@-${ARCH} +o=${NODE_OUTPUT} ${XFBUILD_FLAGS} ${DEBUG_FLAGS} ${NODE_FLAGS} ${NODE_TARGET}

node-release: revision
	xfbuild +D=.deps-$@-${ARCH} +O=.objs-$@-${ARCH} +o=${NODE_OUTPUT} ${XFBUILD_FLAGS} ${RELEASE_FLAGS} ${NODE_FLAGS} ${NODE_TARGET}


# ------------------------------------------------------------------------------
# Upload node - TODO: this is totally out-dated

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
# Cleanup

clean:
	xfbuild ${XFBUILD_FLAGS} +clean ${NODE_TARGET}
	@-rm .objs-* -rf
	@-rm .deps-* -rf
