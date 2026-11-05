//go:build linux && cgo

package main

// The release binary links libstdc++ statically (see the Makefile), and
// libstdc++'s std::random_device, used by DuckDB, calls arc4random, which
// glibc only exports from 2.36. Defining it here, on top of getrandom(2)
// (glibc 2.25), keeps the binary loadable on glibc 2.34 (RHEL 9, Ubuntu
// 22.04). The definition is used only when the
// binary itself resolves the symbol; nothing in Go calls it.

/*
#include <stdint.h>
#include <stddef.h>
#include <errno.h>
#include <sys/random.h>

static void parkana_fill_random(void *buf, size_t n) {
	unsigned char *p = buf;
	while (n > 0) {
		ssize_t got = getrandom(p, n, 0);
		if (got < 0) {
			if (errno == EINTR) {
				continue;
			}
			__builtin_trap();
		}
		p += got;
		n -= (size_t)got;
	}
}

uint32_t arc4random(void) {
	uint32_t v;
	parkana_fill_random(&v, sizeof v);
	return v;
}

void arc4random_buf(void *buf, size_t n) {
	parkana_fill_random(buf, n);
}
*/
import "C"
