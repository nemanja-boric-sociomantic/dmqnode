# ------------------------------------------------------------------------------
# Architecture to build for (32 or 64)

# default
ARCH := 64


# ------------------------------------------------------------------------------
# Targets

DHT_TARGET = src/main/dhtnode.d
DHT_OUTPUT = bin/dhtnode-${ARCH}

QUEUE_TARGET = src/main/queuenode.d
QUEUE_OUTPUT = bin/queuenode-${ARCH}

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
	xfbuild +D=.deps-$@-${ARCH} +O=.objs-$@-${ARCH} +o=${DHT_OUTPUT} ${XFBUILD_FLAGS} ${DEBUG_FLAGS} ${DHT_TARGET}

dht-release: revision
	xfbuild +D=.deps-$@-${ARCH} +O=.objs-$@-${ARCH} +o=${DHT_OUTPUT} ${XFBUILD_FLAGS} ${RELEASE_FLAGS} ${DHT_TARGET}

queue: revision
	xfbuild +D=.deps-$@-${ARCH} +O=.objs-$@-${ARCH} +o=${QUEUE_OUTPUT} ${XFBUILD_FLAGS} ${DEBUG_FLAGS} ${QUEUE_TARGET}

queue-release: revision
	xfbuild +D=.deps-$@-${ARCH} +O=.objs-$@-${ARCH} +o=${QUEUE_OUTPUT} ${XFBUILD_FLAGS} ${RELEASE_FLAGS} ${QUEUE_TARGET}

# ------------------------------------------------------------------------------
# Cleanup

clean:
	xfbuild ${XFBUILD_FLAGS} +clean ${DHT_TARGET}
	xfbuild ${XFBUILD_FLAGS} +clean ${QUEUE_TARGET}
	$(RM) -r .objs-* .deps-*
