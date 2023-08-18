package fusion

import (
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

const (
	alice = "alice"
	bob   = "bob"
)

type store = Store[string, uint64]

var _ store = &testStore{}

type testStore struct {
	data map[string]uint64
}

func newTestStore() *testStore {
	return &testStore{
		data: map[string]uint64{},
	}
}

func (s *testStore) Get(key string) (uint64, bool) {
	v, exists := s.data[key]
	return v, exists
}

func (s *testStore) Set(key string, value uint64) {
	s.data[key] = value
}

func (s *testStore) Delete(key string) {
	delete(s.data, key)
}

func hashingFunc(data string) string {
	return data
}

var (
	errPanic  = errors.New("panic")
	errPanic2 = errors.New("panic2")
)

func toHandlerCh(handlers ...HandlerFunc[string, uint64, string]) <-chan HandlerFunc[string, uint64, string] {
	handlerCh := make(chan HandlerFunc[string, uint64, string], len(handlers))
	for _, handler := range handlers {
		handlerCh <- handler
	}
	close(handlerCh)
	return handlerCh
}

type msgSend struct {
	Sender    string
	Recipient string
	Amount    uint64

	Err error
}

func sendHandler(msg msgSend) HandlerFunc[string, uint64, string] {
	return func(kf KeyFactory[string, uint64, string]) error {
		senderBalanceKey := kf.Key(msg.Sender)
		recipientBalanceKey := kf.Key(msg.Recipient)

		senderBalance, _ := senderBalanceKey.Get()
		recipientBalance, _ := recipientBalanceKey.Get()

		// I don't care about int overflows here
		senderBalance -= msg.Amount
		recipientBalance += msg.Amount

		senderBalanceKey.Set(senderBalance)
		recipientBalanceKey.Set(recipientBalance)

		if errors.Is(msg.Err, errPanic) {
			panic(msg.Err)
		}
		if errors.Is(msg.Err, errPanic2) {
			panic(msg.Err.Error())
		}

		return msg.Err
	}
}

func deleteHandler(err error) HandlerFunc[string, uint64, string] {
	return func(kf KeyFactory[string, uint64, string]) error {
		aliceKey := kf.Key(alice)
		aliceKey.Delete()

		return err
	}
}

func TestSingleTask(t *testing.T) {
	requireT := require.New(t)

	s := newTestStore()
	s.Set(alice, 100)
	s.Set(bob, 50)

	results := do(requireT, s,
		toHandlerCh(
			sendHandler(msgSend{
				Sender:    alice,
				Recipient: bob,
				Amount:    10,
				Err:       nil,
			}),
		),
	)

	requireT.Equal([]error{nil}, results)

	aliceBalance, exists := s.Get(alice)
	requireT.True(exists)
	bobBalance, exists := s.Get(bob)
	requireT.True(exists)

	requireT.Equal(90, int(aliceBalance))
	requireT.Equal(60, int(bobBalance))
}

func TestSingleTaskError(t *testing.T) {
	requireT := require.New(t)

	s := newTestStore()
	s.Set(alice, 100)
	s.Set(bob, 50)

	errTest := errors.New("test error")

	results := do(requireT, s,
		toHandlerCh(
			sendHandler(msgSend{
				Sender:    alice,
				Recipient: bob,
				Amount:    10,
				Err:       errTest,
			}),
		),
	)

	requireT.Equal([]error{errTest}, results)

	aliceBalance, exists := s.Get(alice)
	requireT.True(exists)
	bobBalance, exists := s.Get(bob)
	requireT.True(exists)

	requireT.Equal(100, int(aliceBalance))
	requireT.Equal(50, int(bobBalance))
}

func TestSingleTaskPanic(t *testing.T) {
	requireT := require.New(t)

	s := newTestStore()
	s.Set(alice, 100)
	s.Set(bob, 50)

	results := do(requireT, s,
		toHandlerCh(
			sendHandler(msgSend{
				Sender:    alice,
				Recipient: bob,
				Amount:    10,
				Err:       errPanic,
			}),
		),
	)

	requireT.Equal([]error{errPanic}, results)

	aliceBalance, exists := s.Get(alice)
	requireT.True(exists)
	bobBalance, exists := s.Get(bob)
	requireT.True(exists)

	requireT.Equal(100, int(aliceBalance))
	requireT.Equal(50, int(bobBalance))
}

func TestSingleTaskPanic2(t *testing.T) {
	requireT := require.New(t)

	s := newTestStore()
	s.Set(alice, 100)
	s.Set(bob, 50)

	results := do(requireT, s,
		toHandlerCh(
			sendHandler(msgSend{
				Sender:    alice,
				Recipient: bob,
				Amount:    10,
				Err:       errPanic2,
			}),
		),
	)

	requireT.Len(results, 1)
	requireT.Equal("panic: panic2", results[0].Error())

	aliceBalance, exists := s.Get(alice)
	requireT.True(exists)
	bobBalance, exists := s.Get(bob)
	requireT.True(exists)

	requireT.Equal(100, int(aliceBalance))
	requireT.Equal(50, int(bobBalance))
}

func TestSendThenDelete(t *testing.T) {
	requireT := require.New(t)

	s := newTestStore()
	s.Set(alice, 100)
	s.Set(bob, 50)

	msg := msgSend{
		Sender:    alice,
		Recipient: bob,
		Amount:    10,
	}

	results := do(requireT, s,
		toHandlerCh(
			sendHandler(msg),
			deleteHandler(nil),
		),
	)

	requireT.Equal([]error{nil, nil}, results)

	aliceBalance, exists := s.Get(alice)
	requireT.False(exists)
	bobBalance, exists := s.Get(bob)
	requireT.True(exists)

	requireT.Equal(0, int(aliceBalance))
	requireT.Equal(60, int(bobBalance))
}

func TestDeleteThenSend(t *testing.T) {
	requireT := require.New(t)

	s := newTestStore()
	s.Set(alice, 100)
	s.Set(bob, 50)

	msg := msgSend{
		Sender:    bob,
		Recipient: alice,
		Amount:    10,
	}

	results := do(requireT, s,
		toHandlerCh(
			deleteHandler(nil),
			sendHandler(msg),
		),
	)

	requireT.Equal([]error{nil, nil}, results)

	aliceBalance, exists := s.Get(alice)
	requireT.True(exists)
	bobBalance, exists := s.Get(bob)
	requireT.True(exists)

	requireT.Equal(10, int(aliceBalance))
	requireT.Equal(40, int(bobBalance))
}

func TestDeleteWithError(t *testing.T) {
	requireT := require.New(t)

	s := newTestStore()
	s.Set(alice, 100)
	s.Set(bob, 50)

	errTest := errors.New("test error")

	msg := msgSend{
		Sender:    alice,
		Recipient: bob,
		Amount:    10,
	}

	results := do(requireT, s,
		toHandlerCh(
			sendHandler(msg),
			deleteHandler(errTest),
		),
	)

	requireT.Equal([]error{nil, errTest}, results)

	aliceBalance, exists := s.Get(alice)
	requireT.True(exists)
	bobBalance, exists := s.Get(bob)
	requireT.True(exists)

	requireT.Equal(90, int(aliceBalance))
	requireT.Equal(60, int(bobBalance))
}

func TestManyTasks(t *testing.T) {
	const count = 100

	requireT := require.New(t)

	s := newTestStore()
	s.Set(alice, 100+count*10)
	s.Set(bob, 50)

	handlers := make([]HandlerFunc[string, uint64, string], 0, count)
	expectedResults := make([]error, count)
	for i := 0; i < count; i++ {
		handlers = append(handlers, sendHandler(msgSend{
			Sender:    alice,
			Recipient: bob,
			Amount:    10,
		}))
	}

	results := do(requireT, s, toHandlerCh(handlers...))

	requireT.Equal(expectedResults, results)

	aliceBalance, exists := s.Get(alice)
	requireT.True(exists)
	bobBalance, exists := s.Get(bob)
	requireT.True(exists)

	requireT.Equal(100, int(aliceBalance))
	requireT.Equal(50+count*10, int(bobBalance))
}

func do(
	requireT *require.Assertions,
	s Store[string, uint64],
	handlerCh <-chan HandlerFunc[string, uint64, string],
) []error {
	results, err := Run[string, uint64, string](s, hashingFunc, len(handlerCh), handlerCh)
	requireT.NoError(err)

	return results
}
