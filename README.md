# balance

DHT and Least-Connections load balance for grpc

## Basic Usage

```go
b, _ := balance.NewDHT("http://etcd:2379")
//b, _ := balance.NewLeastConn("http://etcd:2379")
conn, _ := grpc.Dial("service-name", grpc.WithBalancer(b))
```

## Full Example

[server](example/server.go)

[client](example/client.go)