package balance

import "google.golang.org/grpc"

type lcEndpoints struct {
	addr      grpc.Address
	connected bool
	keptConn  int64
}
