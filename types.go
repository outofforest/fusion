package fusion

import (
	"context"
	"fmt"
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

// PanicError is the error type that occurs when a subtask panics.
type PanicError struct {
	Value any
}

func (err PanicError) Error() string {
	return fmt.Sprintf("panic: %s", err.Value)
}

// Unwrap returns the error passed to panic, or nil if panic was called with
// something other than an error.
func (err PanicError) Unwrap() error {
	if e, ok := err.Value.(error); ok {
		return e
	}

	return nil
}
