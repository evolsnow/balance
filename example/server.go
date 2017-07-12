package main

import (
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"math/rand"
	"time"

	"github.com/coreos/etcd/clientv3"
	etcdnaming "github.com/coreos/etcd/clientv3/naming"
	pb "github.com/evolsnow/balance/example/helloworld"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/naming"
)

var (
	port = os.Getenv("PORT")
)

type server struct{}

func (s server) SayHello(ctx context.Context, in *pb.HelloRequest) (*pb.HelloReply, error) {
	time.Sleep(time.Millisecond * time.Duration(rand.Intn(100)))
	return &pb.HelloReply{Message: "Hello " + in.Name}, nil
}

func main() {
	rand.Seed(time.Now().Unix())
	laddr := net.JoinHostPort("127.0.0.1", port)
	lis, err := net.Listen("tcp", laddr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterGreeterServer(s, server{})

	// register server
	e, _ := clientv3.NewFromURL("http://etcd:2379")
	r := &etcdnaming.GRPCResolver{Client: e}
	r.Update(context.TODO(), "service", naming.Update{Op: naming.Add, Addr: laddr})

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		log.Println("listen:", lis.Addr())
		err := s.Serve(lis)
		if err != nil {
			log.Println(err)
		}
	}()

	<-signals
	r.Update(context.TODO(), "service", naming.Update{Op: naming.Delete, Addr: laddr})

}
