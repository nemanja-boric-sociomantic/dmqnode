override LDFLAGS += -llzo2

# Modules to exclude from testing
TEST_FILTER_OUT += \
	$T/src/swarmnodes/queue/main.d \
	$T/src/swarmnodes/dht/memory/dhtdump/main.d \
	$T/src/swarmnodes/dht/memory/main.d \
	$T/src/swarmnodes/dht/logfiles/main.d

$B/dhtnode: override LDFLAGS += -ltokyocabinet
$B/dhtnode: src/swarmnodes/dht/main.d
dht: $B/dhtnode
all += dht

$B/dhtdump: override LDFLAGS += -lebtree
$B/dhtdump: src/swarmnodes/dht/dhtdump/main.d
dhtdump: $B/dhtdump
all += dhtdump

$B/logfilesnode: src/swarmnodes/logfiles/main.d
logfilesnode: $B/logfilesnode
all += logfilesnode

$B/queuenode: src/swarmnodes/queue/main.d
queuenode: $B/queuenode
all += queuenode

# Additional flags needed when unittesting
$O/unittests: override LDFLAGS += -ltokyocabinet -lebtree
