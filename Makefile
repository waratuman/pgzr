PREFIX ?= /usr/local
OPTIMIZE ?= ReleaseFast

.PHONY: all install test clean

all:
	zig build -Doptimize=$(OPTIMIZE)

install:
	zig build -Doptimize=$(OPTIMIZE) --prefix $(PREFIX)

test:
	zig build test

clean:
	rm -rf zig-out zig-cache .zig-cache
