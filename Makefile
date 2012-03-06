# ------------------------------------------------------------------------------
# Architecture to build for (32 or 64)

# default
ARCH := 32


# ------------------------------------------------------------------------------
# Targets

NODE_TARGET = src/main/queuenode.d
NODE_OUTPUT = bin/queuenode-${ARCH}

MONITOR_TARGET = src/main/queuemonitor.d
MONITOR_OUTPUT = bin/queuemonitor-${ARCH}

CONSUMER_TARGET = src/main/queueconsumer.d
CONSUMER_OUTPUT = bin/queueconsumer-${ARCH}

PRODUCER_TARGET = src/main/queueproducer.d
PRODUCER_OUTPUT = bin/queueproducer-${ARCH}

TEST_TARGET = src/main/queuetest.d
TEST_OUTPUT = bin/queuetest-${ARCH}

PERFORMANCE_TARGET = src/main/queueperformance.d
PERFORMANCE_OUTPUT = bin/queueperformance-${ARCH}


# ------------------------------------------------------------------------------
# Dependencies

DEPS_PATH := ..
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
	-L-lglib-2.0 \
	-L-lebtree \
	 -m${ARCH}

TOOL_FLAGS =\
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

.PHONY: revision node monitor consumer test

default: node
#monitor consumer test


# ------------------------------------------------------------------------------
# Revision file build

revision:
	@../ocean/script/mkversion.sh $(D_GC) $(DEPS)


# ------------------------------------------------------------------------------
# Node debug & release builds

node: revision
	xfbuild +o=${NODE_OUTPUT} ${XFBUILD_FLAGS} ${DEBUG_FLAGS} ${NODE_TARGET}


node-release: export D_GC := basic

node-release: revision
	xfbuild +o=${NODE_OUTPUT} ${XFBUILD_FLAGS} ${RELEASE_FLAGS} ${NODE_TARGET}


# ------------------------------------------------------------------------------
# Monitor debug & release builds

monitor: revision
	xfbuild +o=${MONITOR_OUTPUT} ${XFBUILD_FLAGS} ${DEBUG_FLAGS} ${TOOL_FLAGS} ${MONITOR_TARGET}

monitor-release: revision
	xfbuild +o=${MONITOR_OUTPUT} ${XFBUILD_FLAGS} ${RELEASE_FLAGS} ${TOOL_FLAGS} ${MONITOR_TARGET}


# ------------------------------------------------------------------------------
# Consumer debug & release builds

consumer:
	xfbuild +o=${CONSUMER_OUTPUT} ${XFBUILD_FLAGS} ${DEBUG_FLAGS} ${TOOL_FLAGS} ${CONSUMER_TARGET}

consumer-release:
	xfbuild +o=${CONSUMER_OUTPUT} ${XFBUILD_FLAGS} ${RELEASE_FLAGS} ${TOOL_FLAGS} ${CONSUMER_TARGET}


# ------------------------------------------------------------------------------
# Producer debug & release builds

producer:
	xfbuild +o=${PRODUCER_OUTPUT} ${XFBUILD_FLAGS} ${DEBUG_FLAGS} ${TOOL_FLAGS} ${PRODUCER_TARGET}

producer-release:
	xfbuild +o=${PRODUCER_OUTPUT} ${XFBUILD_FLAGS} ${RELEASE_FLAGS} ${TOOL_FLAGS} ${PRODUCER_TARGET}


# ------------------------------------------------------------------------------
# Test debug & release builds

test:
	xfbuild +o=${TEST_OUTPUT} ${XFBUILD_FLAGS} ${DEBUG_FLAGS} ${TOOL_FLAGS} ${TEST_TARGET}

test-release:
	xfbuild +o=${TEST_OUTPUT} ${XFBUILD_FLAGS} ${RELEASE_FLAGS} ${TOOL_FLAGS} ${TEST_TARGET}


# ------------------------------------------------------------------------------
# Performance test debug & release builds

performance:
	xfbuild +o=${PERFORMANCE_OUTPUT} ${XFBUILD_FLAGS} ${DEBUG_FLAGS} ${TOOL_FLAGS} ${PERFORMANCE_TARGET}

performance-release:
	xfbuild +o=${PERFORMANCE_OUTPUT} ${XFBUILD_FLAGS} ${RELEASE_FLAGS} ${TOOL_FLAGS} ${PERFORMANCE_TARGET}


# ------------------------------------------------------------------------------
# Server commands -- upload and connect to server

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
	xfbuild ${XFBUILD_FLAGS} +clean ${MONITOR_TARGET}
	xfbuild ${XFBUILD_FLAGS} +clean ${CONSUMER_TARGET}
	xfbuild ${XFBUILD_FLAGS} +clean ${TEST_TARGET}
	xfbuild ${XFBUILD_FLAGS} +clean ${PERFORMANCE_TARGET}
	@-rm .objs-* .deps-* -rf

