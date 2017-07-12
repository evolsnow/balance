package main

import (
	"context"
	"log"
	"math/rand"
	"os"
	"time"

	"github.com/evolsnow/balance"
	pb "github.com/evolsnow/balance/example/helloworld"
	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"
)

func main() {
	// debug
	l := grpclog.NewLoggerV2(os.Stdout, os.Stdout, os.Stdout)
	grpclog.SetLoggerV2(l)

	rand.Seed(time.Now().Unix())
	//dht()
	lc()
}

func dht() {
	b, err := balance.NewDHT("http://etcd:2379")
	if err != nil {
		log.Fatal(err)
	}
	// to override the default hash function
	//balance.SetHashFunc(func(_ []byte) uint32 {
	//	return rand.Uint32()
	//})
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithBalancer(b))
	opts = append(opts, grpc.WithInsecure())
	// to block on non-available backend server
	opts = append(opts, grpc.WithDefaultCallOptions(grpc.FailFast(false)))
	conn, err := grpc.Dial("service", opts...)
	if err != nil {
		log.Fatal(err)
	}
	cli := pb.NewGreeterClient(conn)
	for {
		time.Sleep(time.Millisecond * time.Duration(rand.Intn(100)))
		key := randStringBytes(12)
		go cli.SayHello(balance.HashKey(context.TODO(), key), new(pb.HelloRequest))
	}
}
func lc() {
	b, err := balance.NewLeastConn("http://etcd:2379")
	if err != nil {
		log.Fatal(err)
	}
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithBalancer(b))
	opts = append(opts, grpc.WithInsecure())
	// to block on non-available backend server
	//opts = append(opts, grpc.WithDefaultCallOptions(grpc.FailFast(false)))
	conn, err := grpc.Dial("service", opts...)
	if err != nil {
		log.Fatal(err)
	}
	cli := pb.NewGreeterClient(conn)
	for {
		time.Sleep(time.Millisecond * time.Duration(rand.Intn(100)))
		go cli.SayHello(context.TODO(), new(pb.HelloRequest))
	}
}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func randStringBytes(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}
