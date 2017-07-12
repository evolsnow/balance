package balance

import (
	"errors"
	"fmt"
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

type dht struct {
	r         naming.Resolver
	w         naming.Watcher
	h         hashFunc
	endpoints []*dhtEndpoint // all the addresses the client should potentially connect
	mu        sync.Mutex
	addrCh    chan []grpc.Address // the channel to notify gRPC internals the list of addresses the client should connect to.
	waitCh    chan struct{}       // the channel to block when there is no connected address available
	done      bool                // The Balancer is closed.
	ring      ring                // consistent hash ring
}

func NewDHT(etcdAddrs ...string) (grpc.Balancer, error) {
	d := &dht{h: defaultHashFunc}
	if len(etcdAddrs) == 0 {
		return d, nil
	}
	cli, err := clientv3.New(clientv3.Config{Endpoints: etcdAddrs})
	if err != nil {
		return nil, err
	}
	d.r = &etcdnaming.GRPCResolver{Client: cli}
	return d, nil
}

func (d *dht) Start(target string, config grpc.BalancerConfig) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.done {
		return grpc.ErrClientConnClosing
	}
	if d.w != nil {
		return errors.New("balancer used twice, please make a new balancer")
	}
	if d.r == nil {
		// If there is no name resolver installed, it is not needed to
		// do name resolution. In this case, target is added into d.endpoints
		// as the only address available and d.addrCh stays nil.
		d.endpoints = append(d.endpoints, &dhtEndpoint{addr: grpc.Address{Addr: target}})
		return nil
	}
	w, err := d.r.Resolve(target)
	if err != nil {
		return err
	}
	d.w = w
	d.addrCh = make(chan []grpc.Address)
	go func() {
		for {
			if err := d.watchAddrUpdates(); err != nil {
				return
			}
		}
	}()
	return nil
}

func (d *dht) Up(addr grpc.Address) (down func(error)) {
	grpclog.Info("balance: addr up:", addr)
	d.mu.Lock()
	defer d.mu.Unlock()
	var cnt int
	for _, a := range d.endpoints {
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
	if cnt == 1 && d.waitCh != nil {
		close(d.waitCh)
		d.waitCh = nil
	}
	grpclog.Info("balance: rebuild ring for up")
	d.rebuildRing()
	return func(err error) {
		grpclog.Info("balance: addr down:", addr, err)
		d.down(addr, err)
	}
}

func (d *dht) Get(ctx context.Context, opts grpc.BalancerGetOptions) (addr grpc.Address, put func(), err error) {
	v, ok := ctx.Value(keyToHash).([]byte)
	if !ok {
		err = status.Errorf(codes.NotFound, "balance: hash key not found in context")
		return
	}
	d.mu.Lock()
	if d.done {
		d.mu.Unlock()
		err = grpc.ErrClientConnClosing
		return
	}
	// ideal situation
	if len(d.ring) > 0 {
		addr = d.ring.choose(d.h(v)).addr
		d.mu.Unlock()
		return
	}
	// do not block on waiting for available endpoints
	if !opts.BlockingWait {
		d.mu.Unlock()
		err = status.Errorf(codes.Unavailable, "balance: there is no address available")
		grpclog.Info("fail fast on empty endpoints")
		return
	}
	var ch chan struct{}
	if d.waitCh == nil {
		ch = make(chan struct{})
		d.waitCh = ch
	} else {
		ch = d.waitCh
	}
	d.mu.Unlock()
	grpclog.Info("waiting for available endpoint")
	for {
		select {
		case <-ctx.Done():
			err = ctx.Err()
			return
		case <-ch:
			d.mu.Lock()
			if d.done {
				d.mu.Unlock()
				err = grpc.ErrClientConnClosing
				return
			}
			if len(d.ring) > 0 {
				grpclog.Info("balance: get() notified by addr up")
				addr = d.ring.choose(d.h(v)).addr
				d.mu.Unlock()
				return
			}
			// The newly added addr got removed by Down() again.
			if d.waitCh == nil {
				ch = make(chan struct{})
				d.waitCh = ch
			} else {
				ch = d.waitCh
			}
			d.mu.Unlock()
		}
	}
}

func (d *dht) Notify() <-chan []grpc.Address {
	return d.addrCh
}

func (d *dht) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.done {
		return errors.New("grpc: balancer is closed")
	}
	d.done = true
	if d.w != nil {
		d.w.Close()
	}
	if d.waitCh != nil {
		close(d.waitCh)
		d.waitCh = nil
	}
	if d.addrCh != nil {
		close(d.addrCh)
	}
	return nil
}

func (d *dht) watchAddrUpdates() error {
	updates, err := d.w.Next()
	if err != nil {
		grpclog.Infof("balance: the naming watcher stops working due to %v.\n", err)
		return err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, update := range updates {
		addr := grpc.Address{
			Addr:     update.Addr,
			Metadata: update.Metadata,
		}
		switch update.Op {
		case naming.Add:
			var exist bool
			for _, v := range d.endpoints {
				if addr == v.addr {
					exist = true
					grpclog.Info("balance: The name resolver wanted to add an existing address: ", addr)
					break
				}
			}
			if exist {
				continue
			}
			d.endpoints = append(d.endpoints, &dhtEndpoint{addr: addr})
		case naming.Delete:
			for i, v := range d.endpoints {
				if addr == v.addr {
					d.endpoints[i], d.endpoints[len(d.endpoints)-1] = d.endpoints[len(d.endpoints)-1], nil
					d.endpoints = d.endpoints[:len(d.endpoints)-1]
					break
				}
			}
		default:
			grpclog.Info("balance: unknown update.Op ", update.Op)
		}
	}
	// Make a copy of d.endpoints and write it onto d.addrCh so that gRPC internals gets notified.
	open := make([]grpc.Address, len(d.endpoints))
	for i, v := range d.endpoints {
		open[i] = v.addr
	}
	if d.done {
		return grpc.ErrClientConnClosing
	}
	d.addrCh <- open
	grpclog.Info("balance: latest addrs: ", open)
	return nil
}

// down unsets the connected state of addr.
func (d *dht) down(addr grpc.Address, err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, a := range d.endpoints {
		if addr == a.addr {
			a.connected = false
			break
		}
	}
	grpclog.Info("balance: rebuild ring for down")
	d.rebuildRing()
}

func (d *dht) rebuildRing() {
	if len(d.endpoints) == 0 {
		grpclog.Info("balance: no endpoints for rebuilding")
		d.ring = d.ring[:0]
		return
	}
	bins := maxEndpoints / (len(d.endpoints))
	var r ring
	for _, ep := range d.endpoints {
		if !ep.connected {
			continue
		}
		ep.parent = ep
		ep.score = d.h([]byte(ep.addr.Addr))
		r = append(r, ep)
		for i := 0; i < bins-1; i++ {
			n := &dhtEndpoint{
				addr:   grpc.Address{Addr: fmt.Sprintf("%s#v:%d", ep.addr.Addr, i)},
				parent: ep,
			}
			n.score = d.h([]byte(n.addr.Addr))
			r = append(r, n)
		}
	}
	r.sort()
	d.ring = r
}
