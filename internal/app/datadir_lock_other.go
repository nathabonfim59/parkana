//go:build !(linux || darwin || freebsd || netbsd || openbsd || dragonfly || illumos)

package app

// LockFile is a no-op on platforms without flock(2): it takes no lock and its
// unlock does nothing. Releases are built for Linux only; on these platforms
// nothing stops two processes of the same role from sharing their
// directories, so keep to one per directory by hand.
func LockFile(path string) (unlock func() error, err error) {
	return func() error { return nil }, nil
}
