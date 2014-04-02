include submodules/ocean/script/common.mk

# ------------------------------------------------------------------------------
# Targets

DHT_SOURCE = src/main/dhtnode.d
DHT_TARGET = bin/dhtnode

LOG_SOURCE = src/main/dhtnode.d
LOG_TARGET = bin/logfilesnode

QUEUE_SOURCE = src/main/queuenode.d
QUEUE_TARGET = bin/queuenode


# ------------------------------------------------------------------------------
# dmd flags

FLAGS =\
	$(DEFAULT_FLAGS)\
    -L-llzo2\
    -version=CDGC

DHT_FLAGS =\
	$(FLAGS)\
    -L-ltokyocabinet

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

.PHONY: default all clean revision dht dht-release log log-release queue-release queue

debug: dht log queue
production: dht-release log-release queue-release


# ------------------------------------------------------------------------------
# node debug & release builds

dht: revision
	xfbuild +D=.deps-$@ +O=.objs-$@-${ARCH} +o=${DHT_TARGET} ${XFBUILD_DEFAULT_FLAGS} ${DHT_FLAGS} ${DEBUG_FLAGS} ${DHT_SOURCE}

dht-release: revision
	xfbuild +D=.deps-$@ +O=.objs-$@-${ARCH} +o=${DHT_TARGET} ${XFBUILD_DEFAULT_FLAGS} ${DHT_FLAGS} ${RELEASE_FLAGS} ${DHT_SOURCE}

log: revision
	xfbuild +D=.deps-$@ +O=.objs-$@-${ARCH} +o=${LOG_TARGET} ${XFBUILD_DEFAULT_FLAGS} ${LOG_FLAGS} ${DEBUG_FLAGS} ${LOG_SOURCE}

log-release: revision
	xfbuild +D=.deps-$@ +O=.objs-$@-${ARCH} +o=${LOG_TARGET} ${XFBUILD_DEFAULT_FLAGS} ${LOG_FLAGS} ${RELEASE_FLAGS} ${LOG_SOURCE}

queue: revision
	xfbuild +D=.deps-$@ +O=.objs-$@-${ARCH} +o=${QUEUE_TARGET} ${XFBUILD_DEFAULT_FLAGS} ${QUEUE_FLAGS} ${DEBUG_FLAGS} ${QUEUE_SOURCE}

queue-release: revision
	xfbuild +D=.deps-$@ +O=.objs-$@-${ARCH} +o=${QUEUE_TARGET} ${XFBUILD_DEFAULT_FLAGS} ${QUEUE_FLAGS} ${RELEASE_FLAGS} ${QUEUE_SOURCE}

# ------------------------------------------------------------------------------
# Cleanup

clean:
	$(RM) -r .objs-* .deps-* $(DHT_TARGET) $(LOG_TARGET) $(QUEUE_TARGET)
