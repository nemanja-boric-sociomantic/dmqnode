# ------------------------------------------------------------------------------
# Targets

NODE_TARGET = src/main/queuenode.d
NODE_OUTPUT = bin/queuenode

MONITOR_TARGET = src/main/queuemonitor.d
MONITOR_OUTPUT = bin/queuemonitor


# ------------------------------------------------------------------------------
# Xfbuild flags

XFBUILD_FLAGS =\
	+c=dmd


# ------------------------------------------------------------------------------
# dmd flags

FLAGS =\
    -Isrc \
    -L-lminilzo \
    -L/usr/lib/libglib-2.0.so \
    -Llibebtree.a

RELEASE_FLAGS = ${FLAGS}\
	-L-s

DEBUG_FLAGS = ${FLAGS}\
	-debug -gc


# ------------------------------------------------------------------------------
# Debug build of node & monitor (default)

default: node monitor


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
# Cleanup

clean:
	xfbuild ${XFBUILD_FLAGS} +clean ${FEED_TARGET}
	xfbuild ${XFBUILD_FLAGS} +clean ${STREAM_TARGET}
	@rm .objs-node -rf
	@rm .deps-node -rf
	@rm .objs-monitor -rf
	@rm .deps-monitor -rf

