override LDFLAGS += -llzo2

# Modules to exclude from testing
TEST_FILTER_OUT += \
	$T/src/queuenode/queue/main.d

$B/queuenode: src/queuenode/queue/main.d
queuenode: $B/queuenode
all += queuenode
