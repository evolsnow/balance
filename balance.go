package balance

import "golang.org/x/net/context"

const maxEndpoints = 1024

type ctxKey struct{}

var keyToHash ctxKey = struct{}{}

type hashFunc func([]byte) uint32

var defaultHashFunc hashFunc = BKDRHash

func HashKey(ctx context.Context, key string) context.Context {
	return context.WithValue(ctx, keyToHash, []byte(key))
}

func BKDRHash(digest []byte) uint32 {
	var seed uint32 = 31
	var h uint32
	for _, d := range digest {
		h = h*seed + uint32(d)
	}
	return h
}

func SetHashFunc(h hashFunc) {
	defaultHashFunc = h
}
