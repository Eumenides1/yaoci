package tcp

import (
	"sync"
	"yaoci/lib/sync/atomic"
)

// EchoHandler echos received line to client, using for test
type EchoHandler struct {
	activeConn sync.Map
	closing    atomic.Boolean
}
