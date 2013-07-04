# ------------------------------------------------------------------------------
# Architecture to build for (32 or 64)

# default
ARCH := 64


# ------------------------------------------------------------------------------
# Targets

DHT_SOURCE = src/main/dhtnode.d
DHT_TARGET = bin/dhtnode-${ARCH}

QUEUE_SOURCE = src/main/queuenode.d
QUEUE_TARGET = bin/queuenode-${ARCH}

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
    -L-ltokyocabinet \
    -m${ARCH} \
    -version=CDGC
# TODO: tokyocabinet should only be linked with the memory node

UNITTESTFLAGS =\
	-unittest \
	-debug=OceanUnitTest

RELEASE_FLAGS = ${FLAGS}\
	-L-s

DEBUG_FLAGS = ${FLAGS} ${UNITTESTFLAGS}\
	-debug -gc

#-debug=Raw
#-debug=ISelectClient
#${UNITTESTFLAGS}

# FIXME: unittests disabled due to an unknown compiler bug which manifested in
# the unittest of ocean.core.ObjectPool


# ------------------------------------------------------------------------------
# Debug build of all targets (default)

.PHONY: revision node

default: dht queue
all: default


# ------------------------------------------------------------------------------
# Revision file build

revision:
	@$(DEPS_PATH)/ocean/script/mkversion.sh -L $(DEPS_PATH) \
		-t $(DEPS_PATH)/ocean/script/appVersion.d.tpl $(D_GC) $(DEPS)


# ------------------------------------------------------------------------------
# node debug & release builds

dht: revision
	xfbuild +D=.deps-$@-${ARCH} +O=.objs-$@-${ARCH} +o=${DHT_TARGET} ${XFBUILD_FLAGS} ${DEBUG_FLAGS} ${DHT_SOURCE}

dht-release: revision
	xfbuild +D=.deps-$@-${ARCH} +O=.objs-$@-${ARCH} +o=${DHT_TARGET} ${XFBUILD_FLAGS} ${RELEASE_FLAGS} ${DHT_SOURCE}

queue: revision
	xfbuild +D=.deps-$@-${ARCH} +O=.objs-$@-${ARCH} +o=${QUEUE_TARGET} ${XFBUILD_FLAGS} ${DEBUG_FLAGS} ${QUEUE_SOURCE}

queue-release: revision
	xfbuild +D=.deps-$@-${ARCH} +O=.objs-$@-${ARCH} +o=${QUEUE_TARGET} ${XFBUILD_FLAGS} ${RELEASE_FLAGS} ${QUEUE_SOURCE}

# ------------------------------------------------------------------------------
# Cleanup

clean:
	$(RM) -r .objs-* .deps-* $(DHT_TARGET) $(QUEUE_TARGET)
