override LDFLAGS += -llzo2

# Modules to exclude from testing
TEST_FILTER_OUT += \
	$T/src/queuenode/queue/main.d \
	$T/src/queuenode/dht/memory/dhtdump/main.d \
	$T/src/queuenode/dht/memory/main.d \
	$T/src/queuenode/dht/logfiles/main.d

$B/dhtnode: override LDFLAGS += -ltokyocabinet
$B/dhtnode: src/queuenode/dht/main.d
dht: $B/dhtnode
all += dht

$B/dhtdump: override LDFLAGS += -lebtree
$B/dhtdump: src/queuenode/dht/dhtdump/main.d
dhtdump: $B/dhtdump
all += dhtdump

$B/logfilesnode: src/queuenode/logfiles/main.d
logfilesnode: $B/logfilesnode
all += logfilesnode

$B/queuenode: src/queuenode/queue/main.d
queuenode: $B/queuenode
all += queuenode

# Additional flags needed when unittesting
$O/unittests: override LDFLAGS += -ltokyocabinet -lebtree
