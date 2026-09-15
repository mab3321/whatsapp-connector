package media

import (
	"fmt"
	"net"
	"sync/atomic"
)

type PortAllocator struct {
	Min, Max int
	next     atomic.Uint32
}

func (p *PortAllocator) Listen() (*net.UDPConn, error) {
	n := p.Max - p.Min + 1
	if n <= 0 {
		return nil, fmt.Errorf("invalid port range")
	}
	start := int(p.next.Add(1)-1) % n
	for i := 0; i < n; i++ {
		port := p.Min + (start+i)%n
		c, e := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4zero, Port: port})
		if e == nil {
			return c, nil
		}
	}
	return nil, fmt.Errorf("no free UDP media port in %d-%d", p.Min, p.Max)
}
