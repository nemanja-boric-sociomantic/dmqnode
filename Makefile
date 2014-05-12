include submodules/ocean/script/common.mk

# ------------------------------------------------------------------------------
# Targets

DHT_SOURCE = src/swarmnodes/dht/DhtNodeServer.d
DHT_TARGET = bin/dhtnode

DHTDUMP_SOURCE = src/swarmnodes/dhtdump/DhtDump.d
DHTDUMP_TARGET = bin/dhtdump

LOG_SOURCE = src/swarmnodes/dht/LogfilesNodeServer.d
LOG_TARGET = bin/logfilesnode

QUEUE_SOURCE = src/swarmnodes/queue/QueueNodeServer.d
QUEUE_TARGET = bin/queuenode


# ------------------------------------------------------------------------------
# dmd flags

FLAGS =\
	$(DEFAULT_FLAGS)\
	-Isrc \
    -L-llzo2\
    -version=CDGC

DHT_FLAGS =\
	$(FLAGS)\
    -L-ltokyocabinet

DHTDUMP_FLAGS =\
	$(FLAGS)\
	-L-lebtree

LOG_FLAGS =\
	$(FLAGS)\
    -L-ltokyocabinet

QUEUE_FLAGS =\
	$(FLAGS)

RELEASE_FLAGS =\
	-L-s

DEBUG_FLAGS =\
	-debug -gc


# ------------------------------------------------------------------------------
# Debug build of all targets (default)

.PHONY: default all clean revision dht dht-release dhtdump dhtdump-releaese log log-release queue-release queue

debug: dht dhtdump log queue
production: dht-release dhtdump-release log-release queue-release


# ------------------------------------------------------------------------------
# node debug & release builds

dht: revision
	xfbuild +D=.deps-$@ +O=.objs-$@ +o=${DHT_TARGET} ${XFBUILD_DEFAULT_FLAGS} ${DHT_FLAGS} ${DEBUG_FLAGS} ${DHT_SOURCE}

dht-release: revision
	xfbuild +D=.deps-$@ +O=.objs-$@ +o=${DHT_TARGET} ${XFBUILD_DEFAULT_FLAGS} ${DHT_FLAGS} ${RELEASE_FLAGS} ${DHT_SOURCE}

dhtdump: revision
	xfbuild +D=.deps-$@ +O=.objs-$@ +o=${DHTDUMP_TARGET} ${XFBUILD_DEFAULT_FLAGS} ${DHTDUMP_FLAGS} ${DEBUG_FLAGS} ${DHTDUMP_SOURCE}

dhtdump-release: revision
	xfbuild +D=.deps-$@ +O=.objs-$@ +o=${DHTDUMP_TARGET} ${XFBUILD_DEFAULT_FLAGS} ${DHTDUMP_FLAGS} ${RELEASE_FLAGS} ${DHTDUMP_SOURCE}

log: revision
	xfbuild +D=.deps-$@ +O=.objs-$@ +o=${LOG_TARGET} ${XFBUILD_DEFAULT_FLAGS} ${LOG_FLAGS} ${DEBUG_FLAGS} ${LOG_SOURCE}

log-release: revision
	xfbuild +D=.deps-$@ +O=.objs-$@ +o=${LOG_TARGET} ${XFBUILD_DEFAULT_FLAGS} ${LOG_FLAGS} ${RELEASE_FLAGS} ${LOG_SOURCE}

queue: revision
	xfbuild +D=.deps-$@ +O=.objs-$@ +o=${QUEUE_TARGET} ${XFBUILD_DEFAULT_FLAGS} ${QUEUE_FLAGS} ${DEBUG_FLAGS} ${QUEUE_SOURCE}

queue-release: revision
	xfbuild +D=.deps-$@ +O=.objs-$@ +o=${QUEUE_TARGET} ${XFBUILD_DEFAULT_FLAGS} ${QUEUE_FLAGS} ${RELEASE_FLAGS} ${QUEUE_SOURCE}

# ------------------------------------------------------------------------------
# Cleanup

clean:
	$(RM) -r .objs-* .deps-* $(DHT_TARGET) $(DHTDUMP_TARGET) $(LOG_TARGET) $(QUEUE_TARGET)
