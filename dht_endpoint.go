package balance

import (
	"sort"

	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"
)

type dhtEndpoint struct {
	addr      grpc.Address
	connected bool
	parent    *dhtEndpoint
	score     uint32
}

type ring []*dhtEndpoint

func (r ring) sort() {
	sort.Slice(r, func(i, j int) bool { return r[i].score < r[j].score })
}

func (r ring) choose(score uint32) *dhtEndpoint {
	idx := sort.Search(len(r), func(i int) bool { return r[i].score >= score })
	if idx >= len(r) {
		idx = 0
	}
	n := r[idx].parent
	grpclog.Infof("balance: selected server %s for score %d", n.addr.Addr, score)
	return n
}
