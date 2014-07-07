override LDFLAGS += -llzo2

# Modules to exclude from testing
TEST_FILTER_OUT += \
	$T/src/swarmnodes/queue/main.d \
	$T/src/swarmnodes/dht/memory/dhtdump/main.d \
	$T/src/swarmnodes/dht/memory/main.d \
	$T/src/swarmnodes/dht/logfiles/main.d

$B/dhtnode: override LDFLAGS += -ltokyocabinet
$B/dhtnode: src/swarmnodes/dht/memory/main.d
dht: $B/dhtnode
all += dht

$B/dhtdump: override LDFLAGS += -lebtree
$B/dhtdump: src/swarmnodes/dht/memory/dhtdump/main.d
dhtdump: $B/dhtdump
all += dhtdump

$B/log: override LDFLAGS += -ltokyocabinet
$B/log: src/swarmnodes/dht/logfiles/main.d
log: $B/log
all += log

$B/queue: src/swarmnodes/queue/main.d
queue: $B/queue
all += queue

# Additional flags needed when unittesting
$U/src/swarmnodes/dht/memory/storage/% \
$U/src/swarmnodes/dht/memory/app/periodic/% \
    : override LDFLAGS += -ltokyocabinet
