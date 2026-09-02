//go:build !darwin && !linux

package api

import (
	"fmt"
	"net"
)

func peerProcessID(net.Conn) (int, error) {
	return 0, fmt.Errorf("peer process inspection is unsupported on this platform")
}
