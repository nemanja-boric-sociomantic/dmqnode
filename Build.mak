override DFLAGS += -w
override LDFLAGS += -llzo2

# Modules to exclude from testing
TEST_FILTER_OUT += \
	$T/src/dmqnode/main.d

$B/dmqnode: src/dmqnode/main.d
$B/dmqnode: override LDFLAGS += -lebtree
dmqnode: $B/dmqnode
all += dmqnode

$B/dmqperformance: override LDFLAGS += -lebtree
$B/dmqperformance: src/dmqperformance/main.d
dmqperformance: $B/dmqperformance
all += dmqperformance

$O/test-dmqtest: dmqnode
$O/test-dmqtest: override LDFLAGS += -lebtree -lrt -lpcre

# Additional flags needed when unittesting
$O/%unittests: override LDFLAGS += -lebtree -lrt -lglib-2.0 -lpcre
