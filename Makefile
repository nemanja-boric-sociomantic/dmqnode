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

# ------------------------------------------------------------------------------
# Xfbuild flags

XFBUILD_FLAGS =\
	+c=dmd


# ------------------------------------------------------------------------------
# dmd flags

FLAGS =\
	-version=NewTango \
	-version=CDGC \
	-Isrc \
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

default: node monitor consumer test


# ------------------------------------------------------------------------------
# Node debug & release builds

node:
	xfbuild +D=.deps-node +O=.objs-node +o=${NODE_OUTPUT} ${XFBUILD_FLAGS} ${DEBUG_FLAGS} ${NODE_TARGET}

node-release:
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
# Server commands -- upload and connect to server

SERVER = 8

upload: upload-node

upload-node:
	scp -C ${NODE_OUTPUT} root@eq6-${SERVER}.sociomantic.com:/tmp/queue

connect:
	ssh root@eq6-${SERVER}.sociomantic.com


# ------------------------------------------------------------------------------
# Cleanup

clean:
	xfbuild ${XFBUILD_FLAGS} +clean ${NODE_TARGET}
	xfbuild ${XFBUILD_FLAGS} +clean ${MONITOR_TARGET}
	xfbuild ${XFBUILD_FLAGS} +clean ${CONSUMER_TARGET}
	xfbuild ${XFBUILD_FLAGS} +clean ${TEST_TARGET}
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

