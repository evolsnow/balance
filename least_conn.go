package balance

import (
	"errors"
	"sync"

	"github.com/coreos/etcd/clientv3"
	etcdnaming "github.com/coreos/etcd/clientv3/naming"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/naming"
	"google.golang.org/grpc/status"
)

type leastConn struct {
	r         naming.Resolver
	w         naming.Watcher
	endpoints []*lcEndpoints // all the addresses the client should potentially connect
	mu        sync.Mutex
	addrCh    chan []grpc.Address // the channel to notify gRPC internals the list of addresses the client should connect to.
	waitCh    chan struct{}       // the channel to block when there is no connected address available
	done      bool                // The Balancer is closed.
	next      *lcEndpoints        //next endpoint to connect
}

func NewLeastConnBalancer(etcdAddrs ...string) (grpc.Balancer, error) {
	lc := &leastConn{}
	if len(etcdAddrs) == 0 {
		return lc, nil
	}
	cli, err := clientv3.New(clientv3.Config{Endpoints: etcdAddrs})
	if err != nil {
		return nil, err
	}
	lc.r = &etcdnaming.GRPCResolver{Client: cli}
	return lc, nil
}

func (lc *leastConn) Start(target string, config grpc.BalancerConfig) error {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	if lc.done {
		return grpc.ErrClientConnClosing
	}
	if lc.w != nil {
		return errors.New("balancer used twice, please make a new balancer")
	}
	if lc.r == nil {
		// If there is no name resolver installed, it is not needed to
		// do name resolution. In this case, target is added into lc.endpoints
		// as the only address available and lc.addrCh stays nil.
		lc.endpoints = append(lc.endpoints, &lcEndpoints{addr: grpc.Address{Addr: target}})
		return nil
	}
	w, err := lc.r.Resolve(target)
	if err != nil {
		return err
	}
	lc.w = w
	lc.addrCh = make(chan []grpc.Address)
	go func() {
		for {
			if err := lc.watchAddrUpdates(); err != nil {
				return
			}
		}
	}()
	return nil
}

// Up sets the connected state of addr and sends notification if there are pending
// Get() calls.
func (lc *leastConn) Up(addr grpc.Address) func(error) {
	grpclog.Info("balance: addr up:", addr)
	lc.mu.Lock()
	defer lc.mu.Unlock()
	var cnt int
	for _, a := range lc.endpoints {
		if a.addr == addr {
			if a.connected {
				return nil
			}
			a.connected = true
		}
		if a.connected {
			cnt++
		}
	}
	// addr is only one which is connected. Notify the Get() callers who are blocking.
	if cnt == 1 && lc.waitCh != nil {
		close(lc.waitCh)
		lc.waitCh = nil
	}
	grpclog.Info("balance: find next for up")
	lc.findNext()
	return func(err error) {
		grpclog.Info("balance: addr down:", addr, err)
		lc.down(addr, err)
	}
}

// down unsets the connected state of addr.
func (lc *leastConn) down(addr grpc.Address, err error) {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	for _, a := range lc.endpoints {
		if addr == a.addr {
			a.connected = false
			break
		}
	}
	grpclog.Info("balance: find next for down")
	lc.findNext()
}

// Get returns the next addr in the rotation.
func (lc *leastConn) Get(ctx context.Context, opts grpc.BalancerGetOptions) (addr grpc.Address, put func(), err error) {
	defer func() {
		if addr.Addr != "" {
			grpclog.Infof("selected server %s", addr.Addr)
		}
	}()
	lc.mu.Lock()
	if lc.done {
		lc.mu.Unlock()
		err = grpc.ErrClientConnClosing
		return
	}
	lc.findNext()
	if lc.next != nil {
		next := lc.next
		next.keptConn++
		lc.mu.Unlock()
		return next.addr, func() { lc.put(next) }, nil
	}

	if !opts.BlockingWait {
		lc.mu.Unlock()
		err = status.Error(codes.Unavailable, "balance: there is no address available")
		grpclog.Info("fail fast on empty endpoints")
		return
	}
	// Wait on lc.waitCh for non-failfast RPCs.
	var ch chan struct{}
	if lc.waitCh == nil {
		ch = make(chan struct{})
		lc.waitCh = ch
	} else {
		ch = lc.waitCh
	}
	lc.mu.Unlock()
	grpclog.Info("waiting for available endpoint")
	for {
		select {
		case <-ctx.Done():
			err = ctx.Err()
			return
		case <-ch:
			grpclog.Info("notified by up")
			lc.mu.Lock()
			if lc.done {
				lc.mu.Unlock()
				err = grpc.ErrClientConnClosing
				return
			}
			if lc.next != nil {
				next := lc.next
				next.keptConn++
				lc.mu.Unlock()
				grpclog.Info("balance: get() notified by addr up")
				return next.addr, func() { lc.put(next) }, nil
			}
			// The newly added addr got removed by Down() again.
			if lc.waitCh == nil {
				ch = make(chan struct{})
				lc.waitCh = ch
			} else {
				ch = lc.waitCh
			}
			lc.mu.Unlock()
		}
	}
}

func (lc *leastConn) put(ep *lcEndpoints) {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	ep.keptConn--
}

func (lc *leastConn) Notify() <-chan []grpc.Address {
	return lc.addrCh
}

func (lc *leastConn) Close() error {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	if lc.done {
		return errors.New("grpc: balancer is closed")
	}
	lc.done = true
	if lc.w != nil {
		lc.w.Close()
	}
	if lc.waitCh != nil {
		close(lc.waitCh)
		lc.waitCh = nil
	}
	if lc.addrCh != nil {
		close(lc.addrCh)
	}
	return nil
}

func (lc *leastConn) watchAddrUpdates() error {
	updates, err := lc.w.Next()
	if err != nil {
		grpclog.Infof("balance: the naming watcher stops working due to %v.\n", err)
		return err
	}
	lc.mu.Lock()
	defer lc.mu.Unlock()
	for _, update := range updates {
		addr := grpc.Address{
			Addr:     update.Addr,
			Metadata: update.Metadata,
		}
		switch update.Op {
		case naming.Add:
			var exist bool
			for _, v := range lc.endpoints {
				if addr == v.addr {
					exist = true
					grpclog.Info("balance: The name resolver wanted to add an existing address: ", addr)
					break
				}
			}
			if exist {
				continue
			}
			lc.endpoints = append(lc.endpoints, &lcEndpoints{addr: addr, keptConn: 0})
		case naming.Delete:
			for i, v := range lc.endpoints {
				if addr == v.addr {
					lc.endpoints[i], lc.endpoints[len(lc.endpoints)-1] = lc.endpoints[len(lc.endpoints)-1], nil
					lc.endpoints = lc.endpoints[:len(lc.endpoints)-1]
					break
				}
			}
		default:
			grpclog.Info("balance: unknown update.Op ", update.Op)
		}
	}
	// Make a copy of lc.endpoints and write it onto lc.addrCh so that gRPC internals gets notified.
	open := make([]grpc.Address, len(lc.endpoints))
	for i, v := range lc.endpoints {
		open[i] = v.addr
	}
	if lc.done {
		return grpc.ErrClientConnClosing
	}
	lc.addrCh <- open
	grpclog.Info("balance: latest addrs: ", open)
	return nil
}

func (lc *leastConn) findNext() {
	if len(lc.endpoints) == 0 {
		lc.next = nil
		return
	}
	lc.next = lc.endpoints[0]
	found := false
	for _, v := range lc.endpoints {
		if v.connected && v.keptConn <= lc.next.keptConn {
			lc.next = v
			found = true
		}
	}
	if !found {
		lc.next = nil
	}
}
