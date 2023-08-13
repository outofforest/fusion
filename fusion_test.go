package fusion_test

import (
	"context"

	"github.com/outofforest/fusion"
)

type store = fusion.Store[string, uint64]

var _ store = testStore{}

type testStore struct {
	data map[string]uint64
}

//nolint:unused
func newTestStore() testStore {
	return testStore{
		data: map[string]uint64{},
	}
}

func (s testStore) Get(key string) (uint64, bool) {
	v, exists := s.data[key]
	return v, exists
}

func (s testStore) Set(key string, value uint64) {
	s.data[key] = value
}

func (s testStore) Delete(key string) {
	delete(s.data, key)
}

//nolint:unused
type msgSend struct {
	Sender    string
	Recipient string
	Amount    uint64
}

//nolint:unused
func sendHandler(msg msgSend) fusion.HandlerFunc[string, uint64] {
	return func(ctx context.Context, store fusion.Store[string, uint64]) error {
		senderBalance, _ := store.Get(msg.Sender)
		recipientBalance, _ := store.Get(msg.Recipient)

		// I don't care about int overflows here
		store.Set(msg.Sender, senderBalance-msg.Amount)
		store.Set(msg.Recipient, recipientBalance+msg.Amount)

		return nil
	}
}
