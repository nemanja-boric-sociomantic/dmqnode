# ------------------------------------------------------------------------------
# Architecture to build for (32 or 64)

# default
ARCH := 64


# ------------------------------------------------------------------------------
# Targets

NODE_TARGET = src/main/queuenode.d
NODE_OUTPUT = bin/queuenode-${ARCH}


# ------------------------------------------------------------------------------
# Dependencies

DEPS_PATH := ../..
DEPS := tango ocean swarm


# ------------------------------------------------------------------------------
# Xfbuild flags

XFBUILD_FLAGS =\
	+c=dmd \
	+D=.deps-$@-${ARCH} +O=.objs-$@-${ARCH}

# ------------------------------------------------------------------------------
# GC to use (export is needed!)

export D_GC := cdgc


# ------------------------------------------------------------------------------
# dmd flags

FLAGS =\
	$(foreach d,$(DEPS),-I$(DEPS_PATH)/$d) \
	-L-lminilzo \
	-L-lebtree \
	-m${ARCH} \
	-version=CDGC

RELEASE_FLAGS = ${FLAGS}\
	-L-s

DEBUG_FLAGS = ${FLAGS}\
	-debug -gc

#-debug=ISelectClient
#-debug=ConnectionHandler
#-debug=Raw


# ------------------------------------------------------------------------------
# Debug build of node & monitor (default)

.PHONY: revision node

default: node
all: default


# ------------------------------------------------------------------------------
# Revision file build

revision:
	@${DEPS_PATH}/ocean/script/mkversion.sh -t \
	$(DEPS_PATH)/ocean/script/appVersion.d.tpl -L ../.. $(D_GC) $(DEPS)

# ------------------------------------------------------------------------------
# Node debug & release builds

node: revision
	xfbuild +o=${NODE_OUTPUT} ${XFBUILD_FLAGS} ${DEBUG_FLAGS} ${NODE_TARGET}

node-release: revision
	xfbuild +o=${NODE_OUTPUT} ${XFBUILD_FLAGS} ${RELEASE_FLAGS} ${NODE_TARGET}


# ------------------------------------------------------------------------------
# Server commands -- upload and connect to server
# TODO: this is out of date

EU_SERVERS = 8

US_SERVERS = 10 13

upload-monitor-eu:
	$(foreach srv, $(EU_SERVERS), scp -C ${MONITOR_OUTPUT} root@eq6-$(srv).sociomantic.com:/tmp/queue;)

upload-monitor-us:
	$(foreach srv, $(US_SERVERS), scp -C ${MONITOR_OUTPUT} root@is-$(srv).sociomantic.com:/tmp/queue;)

upload-node-eu:
	$(foreach srv, $(EU_SERVERS), scp -C ${NODE_OUTPUT} root@eq6-$(srv).sociomantic.com:/tmp/queue;)

upload-node-us:
	$(foreach srv, $(US_SERVERS), scp -C ${NODE_OUTPUT} root@is-$(srv).sociomantic.com:/tmp/queue;)

connect-eu:
	@../ocean/script/tmuxconnect.sh eu_queue_servers eu ${EU_SERVERS}

connect-us:
	@../ocean/script/tmuxconnect.sh us_queue_servers us ${US_SERVERS}

connect-node-eu:
	@../ocean/script/tmuxconnectscreen.sh eu_queue eu queue ${EU_SERVERS}

connect-node-us:
	@../ocean/script/tmuxconnectscreen.sh us_queue us queue ${US_SERVERS}


# ------------------------------------------------------------------------------
# Cleanup

clean:
	xfbuild ${XFBUILD_FLAGS} +clean ${NODE_TARGET}
	@-rm .objs-* .deps-* -rf

