package fusion

import (
	"context"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

var keys = []string{
	"alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel", "india", "juliett", "kilo", "lima", "mike",
	"november", "oscar", "papa", "quebec", "romeo", "sierra", "tango", "uniform", "victor", "whiskey", "x-ray", "yankee",
	"zulu",
}

func handler(ctx context.Context, store Store[string, uint64]) error {
	key1 := keys[rand.Intn(len(keys))]
	key2 := keys[rand.Intn(len(keys))]
	key3 := keys[rand.Intn(len(keys))]
	key4 := keys[rand.Intn(len(keys))]
	key5 := keys[rand.Intn(len(keys))]
	// keyRand1 := key1 + key2 + key3 + key4 + key5
	// keyRand2 := key2 + key3 + key4 + key5 + key1
	// keyRand3 := key3 + key4 + key5 + key1 + key2
	// keyRand4 := key4 + key5 + key1 + key2 + key3
	// keyRand5 := key5 + key1 + key2 + key3 + key4

	// store.Get(keyRand1)
	// store.Set(keyRand1, 0)
	// store.Get(keyRand2)
	// store.Set(keyRand2, 0)
	// store.Get(keyRand3)
	// store.Set(keyRand3, 0)
	// store.Get(keyRand4)
	// store.Set(keyRand4, 0)
	// store.Get(keyRand5)
	// store.Set(keyRand5, 0)

	store.Get(key1)
	store.Set(key1, 0)
	store.Get(key2)
	store.Set(key2, 0)
	store.Get(key3)
	store.Set(key3, 0)
	store.Get(key4)
	store.Set(key4, 0)
	store.Get(key5)
	store.Set(key5, 0)

	return nil
}

// go test -bench=. -cpuprofile profile.out
// go tool pprof -http="localhost:8000" pprofbin ./profile.out

func BenchmarkFusion(b *testing.B) {
	const count = 10000

	requireT := require.New(b)
	ctx := newContext(b)

	s := newTestStore()

	handlers := make([]HandlerFunc[string, uint64], 0, count)
	expectedResults := make([]error, count)
	for i := 0; i < count; i++ {
		handlers = append(handlers, handler)
	}

	b.ResetTimer()
	results := do(ctx, requireT, s, nil, toHandlerCh(handlers...))
	b.StopTimer()

	requireT.Equal(expectedResults, results)
}
