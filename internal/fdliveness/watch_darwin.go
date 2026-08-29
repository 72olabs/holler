//go:build darwin

package fdliveness

import "golang.org/x/sys/unix"

const peerCloseEvents = unix.POLLERR | unix.POLLHUP | unix.POLLNVAL
