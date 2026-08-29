//go:build linux

package fdliveness

import "golang.org/x/sys/unix"

// Linux AF_UNIX stream sockets report a peer half-close as POLLRDHUP|POLLIN,
// not necessarily POLLHUP.
const peerCloseEvents = unix.POLLERR | unix.POLLHUP | unix.POLLNVAL | unix.POLLRDHUP
