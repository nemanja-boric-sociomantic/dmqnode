# ------------------------------------------------------------------------------
# Targets

NODE_TARGET = src/main/queuenode.d
NODE_OUTPUT = bin/queuenode

MONITOR_TARGET = src/main/queuemonitor.d
MONITOR_OUTPUT = bin/queuemonitor

CONSUMER_TARGET = src/main/queueconsumer.d
CONSUMER_OUTPUT = bin/queueconsumer

PRODUCER_TARGET = src/main/queueproducer.d
PRODUCER_OUTPUT = bin/queueproducer

TEST_TARGET = src/main/queuetest.d
TEST_OUTPUT = bin/queuetest

PERFORMANCE_TARGET = src/main/queueperformance.d
PERFORMANCE_OUTPUT = bin/queueperformance


# ------------------------------------------------------------------------------
# Xfbuild flags

XFBUILD_FLAGS =\
	+c=dmd


# ------------------------------------------------------------------------------
# dmd flags

FLAGS =\
	-J.\
	-version=NewTango \
	-Isrc \
    -I../swarm \
	-L-lminilzo \
	-L-lglib-2.0 \
	-L-lebtree

RELEASE_FLAGS = ${FLAGS}\
	-L-s

DEBUG_FLAGS = ${FLAGS}\
	-debug -gc
#-debug=ConnectionHandler
#-debug=Raw
#-debug=ISelectClient


# ------------------------------------------------------------------------------
# Debug build of node & monitor (default)

.PHONY: revision node monitor consumer test

default: node monitor consumer test


# ------------------------------------------------------------------------------
# Revision file build

DEP_BASE_DIR = ../
DEPENDENCIES = ocean swarm tango

revision:
	@logname > revisions.txt
	@date >> revisions.txt
	@svnversion >> revisions.txt
	@$(foreach x,$(DEPENDENCIES), echo $(x) $$(svnversion $(DEP_BASE_DIR)$(x)) >> revisions.txt;)
	@touch $(DEP_BASE_DIR)ocean/util/Version.d


# ------------------------------------------------------------------------------
# Node debug & release builds

node: revision
	xfbuild +D=.deps-node +O=.objs-node +o=${NODE_OUTPUT} ${XFBUILD_FLAGS} ${DEBUG_FLAGS} ${NODE_TARGET}

node-release: revision
	xfbuild +D=.deps-node +O=.objs-node +o=${NODE_OUTPUT} ${XFBUILD_FLAGS} ${RELEASE_FLAGS} ${NODE_TARGET}


# ------------------------------------------------------------------------------
# Monitor debug & release builds

monitor:
	xfbuild +D=.deps-monitor +O=.objs-monitor +o=${MONITOR_OUTPUT} ${XFBUILD_FLAGS} ${DEBUG_FLAGS} ${MONITOR_TARGET}

monitor-release:
	xfbuild +D=.deps-monitor +O=.objs-monitor +o=${MONITOR_OUTPUT} ${XFBUILD_FLAGS} ${RELEASE_FLAGS} ${MONITOR_TARGET}


# ------------------------------------------------------------------------------
# Consumer debug & release builds

consumer:
	xfbuild +D=.deps-consumer +O=.objs-consumer +o=${CONSUMER_OUTPUT} ${XFBUILD_FLAGS} ${DEBUG_FLAGS} ${CONSUMER_TARGET}

consumer-release:
	xfbuild +D=.deps-consumer +O=.objs-consumer +o=${CONSUMER_OUTPUT} ${XFBUILD_FLAGS} ${RELEASE_FLAGS} ${CONSUMER_TARGET}


# ------------------------------------------------------------------------------
# Consumer debug & release builds

producer:
	xfbuild +D=.deps-producer +O=.objs-producer +o=${PRODUCER_OUTPUT} ${XFBUILD_FLAGS} ${DEBUG_FLAGS} ${PRODUCER_TARGET}

producer-release:
	xfbuild +D=.deps-producer +O=.objs-producer +o=${PRODUCER_OUTPUT} ${XFBUILD_FLAGS} ${RELEASE_FLAGS} ${PRODUCER_TARGET}


# ------------------------------------------------------------------------------
# Test debug & release builds

test:
	xfbuild +D=.deps-test +O=.objs-test +o=${TEST_OUTPUT} ${XFBUILD_FLAGS} ${DEBUG_FLAGS} ${TEST_TARGET}

test-release:
	xfbuild +D=.deps-test +O=.objs-test +o=${TEST_OUTPUT} ${XFBUILD_FLAGS} ${RELEASE_FLAGS} ${TEST_TARGET}


# ------------------------------------------------------------------------------
# Performance test debug & release builds

performance:
	xfbuild +D=.deps-performance +O=.objs-performance +o=${PERFORMANCE_OUTPUT} ${XFBUILD_FLAGS} ${DEBUG_FLAGS} ${PERFORMANCE_TARGET}

performance-release:
	xfbuild +D=.deps-performance +O=.objs-performance +o=${PERFORMANCE_OUTPUT} ${XFBUILD_FLAGS} ${RELEASE_FLAGS} ${PERFORMANCE_TARGET}


# ------------------------------------------------------------------------------
# Server commands -- upload and connect to server

EU_SERVER = 8

US_SERVER = 1

upload-node-eu:
	scp -C ${NODE_OUTPUT} root@eq6-${EU_SERVER}.sociomantic.com:/tmp/queue

upload-node-us:
	scp -C ${NODE_OUTPUT} root@rs-${US_SERVER}.sociomantic.com:/tmp/queue

connect-eu:
	ssh root@eq6-${EU_SERVER}.sociomantic.com

connect-us:
	ssh root@eq6-${US_SERVER}.sociomantic.com

connect-main-eu:
	ssh -t root@eq6-${EU_SERVER}.sociomantic.com "screen -rx queue"

connect-main-us:
	ssh -t root@rs-${US_SERVER}.sociomantic.com "screen -rx queue"


# ------------------------------------------------------------------------------
# Cleanup

clean:
	xfbuild ${XFBUILD_FLAGS} +clean ${NODE_TARGET}
	xfbuild ${XFBUILD_FLAGS} +clean ${MONITOR_TARGET}
	xfbuild ${XFBUILD_FLAGS} +clean ${CONSUMER_TARGET}
	xfbuild ${XFBUILD_FLAGS} +clean ${TEST_TARGET}
	xfbuild ${XFBUILD_FLAGS} +clean ${PERFORMANCE_TARGET}
	@-rm .objs-node -rf
	@-rm .deps-node -rf
	@-rm .objs-monitor -rf
	@-rm .deps-monitor -rf
	@-rm .objs-consumer -rf
	@-rm .deps-consumer -rf
	@-rm .objs-producer -rf
	@-rm .deps-producer -rf
	@-rm .objs-test -rf
	@-rm .deps-test -rf
	@-rm .objs-performance -rf
	@-rm .deps-performance -rf

