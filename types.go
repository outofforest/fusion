package fusion

import (
	"context"
)

// Store is the interface required from store.
type Store[TKey, TValue any] interface {
	Get(key TKey) (TValue, bool)
	Set(key TKey, value TValue)
	Delete(key TKey)
}

// HashingFunc is the function returning hash of key.
type HashingFunc[TKey any, THash comparable] func(key TKey) THash

// HandlerFunc executes the store transformation logic.
type HandlerFunc[TKey, TValue any] func(ctx context.Context, store Store[TKey, TValue]) error
