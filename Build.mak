override LDFLAGS += -llzo2

# Modules to exclude from testing
TEST_FILTER_OUT += \
	$T/src/dmqnode/main.d

$B/dmqnode: src/dmqnode/main.d
dmqnode: $B/dmqnode
all += dmqnode
