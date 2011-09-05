# ------------------------------------------------------------------------------
# Targets

NODE_TARGET = src/main/dhtnode.d
NODE_OUTPUT = bin/dhtnode

REMOVE_TARGET = src/main/dhtremove.d
REMOVE_OUTPUT = bin/dhtremove

INFO_TARGET = src/main/dhtinfo.d
INFO_OUTPUT = bin/dhtinfo


# ------------------------------------------------------------------------------
# Xfbuild flags

XFBUILD_FLAGS =\
	+c=dmd


# ------------------------------------------------------------------------------
# dmd flags

FLAGS =\
	-L-lminilzo \
    -L-ltokyocabinet \
    -L/usr/lib/libglib-2.0.so \
	-L-lebtree \
    -L-ldl \
    -version=NewTango

RELEASE_FLAGS = ${FLAGS}\
	-L-s

DEBUG_FLAGS = ${FLAGS}\
	-debug -gc -debug=ConnectionHandler
#-debug=Raw


# ------------------------------------------------------------------------------
# Debug build of all targets (default)

default: node remove info


# ------------------------------------------------------------------------------
# node debug & release builds

node:
	xfbuild +D=.deps-node +O=.objs-node +o=${NODE_OUTPUT} ${XFBUILD_FLAGS} ${DEBUG_FLAGS} ${NODE_TARGET}

node-release:
	xfbuild +D=.deps-node +O=.objs-node +o=${NODE_OUTPUT} ${XFBUILD_FLAGS} ${RELEASE_FLAGS} ${NODE_TARGET}


# ------------------------------------------------------------------------------
# remove debug & release builds

remove:
	xfbuild +D=.deps-remove +O=.objs-remove +o=${REMOVE_OUTPUT} ${XFBUILD_FLAGS} ${DEBUG_FLAGS} ${REMOVE_TARGET}

remove-release:
	xfbuild +D=.deps-remove +O=.objs-remove +o=${REMOVE_OUTPUT} ${XFBUILD_FLAGS} ${RELEASE_FLAGS} ${REMOVE_TARGET}


# ------------------------------------------------------------------------------
# info debug & release builds

info:
	xfbuild +D=.deps-info +O=.objs-info +o=${INFO_OUTPUT} ${XFBUILD_FLAGS} ${DEBUG_FLAGS} ${INFO_TARGET}

info-release:
	xfbuild +D=.deps-info +O=.objs-info +o=${INFO_OUTPUT} ${XFBUILD_FLAGS} ${RELEASE_FLAGS} ${INFO_TARGET}


# ------------------------------------------------------------------------------
# Cleanup

clean:
	xfbuild ${XFBUILD_FLAGS} +clean ${NODE_TARGET}
	xfbuild ${XFBUILD_FLAGS} +clean ${REMOVE_TARGET}
	xfbuild ${XFBUILD_FLAGS} +clean ${INFO_TARGET}
	@-rm .objs-node -rf
	@-rm .deps-node -rf
	@-rm .objs-remove -rf
	@-rm .deps-remove -rf
	@-rm .objs-info -rf
	@-rm .deps-info -rf

