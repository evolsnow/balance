package balance

import (
	"sort"

	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"
)

type endpoint struct {
	addr      grpc.Address
	connected bool
	parent    *endpoint
	score     uint32
}

type ring []*endpoint

func (r ring) sort() {
	sort.Slice(r, func(i, j int) bool { return r[i].score < r[j].score })
}

func (r ring) choose(score uint32) *endpoint {
	idx := sort.Search(len(r), func(i int) bool { return r[i].score >= score })
	if idx >= len(r) {
		idx = 0
	}
	n := r[idx].parent
	grpclog.Infof("balance: selected server %s for score %d", n.addr.Addr, score)
	return n
}
