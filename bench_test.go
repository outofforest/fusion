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

func handler(ctx context.Context, kf KeyFactory[string, uint64, string]) error {
	key1 := kf.Key(keys[rand.Intn(len(keys))])
	key2 := kf.Key(keys[rand.Intn(len(keys))])
	key3 := kf.Key(keys[rand.Intn(len(keys))])
	key4 := kf.Key(keys[rand.Intn(len(keys))])
	key5 := kf.Key(keys[rand.Intn(len(keys))])
	kf.Seal()

	key1.Get()
	key1.Set(0)
	key2.Get()
	key2.Set(0)
	key3.Get()
	key3.Set(0)
	key4.Get()
	key4.Set(0)
	key5.Get()
	key5.Set(0)

	return nil
}

// go test -bench=. -cpuprofile profile.out
// go tool pprof -http="localhost:8000" pprofbin ./profile.out

func BenchmarkFusion(b *testing.B) {
	const count = 10000

	requireT := require.New(b)
	ctx := newContext(b)

	s := newTestStore()

	handlers := make([]HandlerFunc[string, uint64, string], 0, count)
	expectedResults := make([]error, count)
	for i := 0; i < count; i++ {
		handlers = append(handlers, handler)
	}

	b.ResetTimer()
	results := do(ctx, requireT, s, toHandlerCh(handlers...))
	b.StopTimer()

	requireT.Equal(expectedResults, results)
}
