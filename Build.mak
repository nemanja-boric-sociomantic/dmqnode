override DFLAGS += -w
override LDFLAGS += -llzo2 -lebtree -lrt -lgcrypt -lgpg-error -lglib-2.0

# Modules to exclude from testing
TEST_FILTER_OUT += \
	$T/src/dmqnode/main.d

$B/dmqnode: src/dmqnode/main.d
dmqnode: $B/dmqnode
all += dmqnode

$B/dmqperformance: src/dmqperformance/main.d
dmqperformance: $B/dmqperformance
all += dmqperformance

$O/test-dmqtest: dmqnode
$O/test-dmqtest: override LDFLAGS += -lpcre

# Additional flags needed when unittesting
#$O/%unittests: override LDFLAGS += 
