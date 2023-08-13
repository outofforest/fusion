package fusion

import (
	"context"
	"testing"

	"github.com/outofforest/logger"
	"github.com/outofforest/parallel"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

const (
	alice = "alice"
	bob   = "bob"
)

type store = Store[string, uint64]

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

func hashingFunc(data string) string {
	return data
}

var (
	errPanic  = errors.New("panic")
	errPanic2 = errors.New("panic2")
)

func closedChan() chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}

func newContext(t *testing.T) context.Context {
	ctx, cancel := context.WithCancel(logger.WithLogger(context.Background(), logger.New(logger.DefaultConfig)))
	t.Cleanup(cancel)

	return ctx
}

type msgSend struct {
	Sender    string
	Recipient string
	Amount    uint64

	DoReadingCh   chan struct{}
	ReadingDoneCh chan struct{}
	DoWritingCh   chan struct{}
	WritingDoneCh chan struct{}
	Err           error
}

func sendHandler(msg msgSend) HandlerFunc[string, uint64] {
	return func(ctx context.Context, store Store[string, uint64]) error {
		<-msg.DoReadingCh

		senderBalance, _ := store.Get(msg.Sender)
		recipientBalance, _ := store.Get(msg.Recipient)

		msg.ReadingDoneCh <- struct{}{}

		// I don't care about int overflows here
		senderBalance -= msg.Amount
		recipientBalance += msg.Amount

		<-msg.DoWritingCh

		store.Set(msg.Sender, senderBalance)
		store.Set(msg.Recipient, recipientBalance)

		msg.WritingDoneCh <- struct{}{}

		if errors.Is(msg.Err, errPanic) {
			panic(msg.Err)
		}
		if errors.Is(msg.Err, errPanic2) {
			panic(msg.Err.Error())
		}

		return msg.Err
	}
}

func deleteHandler(err error) HandlerFunc[string, uint64] {
	return func(ctx context.Context, store Store[string, uint64]) error {
		store.Delete(alice)
		return err
	}
}

func TestSingleTask(t *testing.T) {
	requireT := require.New(t)
	ctx := newContext(t)

	s := newTestStore()
	s.Set(alice, 100)
	s.Set(bob, 50)

	results := do(ctx, requireT, s, nil,
		sendHandler(msgSend{
			Sender:        alice,
			Recipient:     bob,
			Amount:        10,
			DoReadingCh:   closedChan(),
			ReadingDoneCh: make(chan struct{}, 1),
			DoWritingCh:   closedChan(),
			WritingDoneCh: make(chan struct{}, 1),
			Err:           nil,
		}),
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
	ctx := newContext(t)

	s := newTestStore()
	s.Set(alice, 100)
	s.Set(bob, 50)

	errTest := errors.New("test error")

	results := do(ctx, requireT, s, nil,
		sendHandler(msgSend{
			Sender:        alice,
			Recipient:     bob,
			Amount:        10,
			DoReadingCh:   closedChan(),
			ReadingDoneCh: make(chan struct{}, 1),
			DoWritingCh:   closedChan(),
			WritingDoneCh: make(chan struct{}, 1),
			Err:           errTest,
		}),
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
	ctx := newContext(t)

	s := newTestStore()
	s.Set(alice, 100)
	s.Set(bob, 50)

	results := do(ctx, requireT, s, nil,
		sendHandler(msgSend{
			Sender:        alice,
			Recipient:     bob,
			Amount:        10,
			DoReadingCh:   closedChan(),
			ReadingDoneCh: make(chan struct{}, 1),
			DoWritingCh:   closedChan(),
			WritingDoneCh: make(chan struct{}, 1),
			Err:           errPanic,
		}),
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
	ctx := newContext(t)

	s := newTestStore()
	s.Set(alice, 100)
	s.Set(bob, 50)

	results := do(ctx, requireT, s, nil,
		sendHandler(msgSend{
			Sender:        alice,
			Recipient:     bob,
			Amount:        10,
			DoReadingCh:   closedChan(),
			ReadingDoneCh: make(chan struct{}, 1),
			DoWritingCh:   closedChan(),
			WritingDoneCh: make(chan struct{}, 1),
			Err:           errPanic2,
		}),
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

func TestTwoTasksWithoutRepeating(t *testing.T) {
	requireT := require.New(t)
	ctx := newContext(t)

	s := newTestStore()
	s.Set(alice, 100)
	s.Set(bob, 50)

	msg1 := msgSend{
		Sender:        alice,
		Recipient:     bob,
		Amount:        10,
		DoReadingCh:   closedChan(),
		ReadingDoneCh: make(chan struct{}, 1),
		DoWritingCh:   closedChan(),
		WritingDoneCh: make(chan struct{}, 1),
	}
	msg2 := msgSend{
		Sender:        alice,
		Recipient:     bob,
		Amount:        10,
		DoReadingCh:   make(chan struct{}, 1),
		ReadingDoneCh: make(chan struct{}, 1),
		DoWritingCh:   make(chan struct{}, 1),
		WritingDoneCh: make(chan struct{}, 1),
	}

	results := do(ctx, requireT, s,
		func() {
			<-msg1.WritingDoneCh
			msg2.DoReadingCh <- struct{}{}
			msg2.DoWritingCh <- struct{}{}
		},
		sendHandler(msg1),
		sendHandler(msg2),
	)

	requireT.Equal([]error{nil, nil}, results)

	aliceBalance, exists := s.Get(alice)
	requireT.True(exists)
	bobBalance, exists := s.Get(bob)
	requireT.True(exists)

	requireT.Equal(80, int(aliceBalance))
	requireT.Equal(70, int(bobBalance))
}

func TestTwoTasksWithRepeating(t *testing.T) {
	requireT := require.New(t)
	ctx := newContext(t)

	s := newTestStore()
	s.Set(alice, 100)
	s.Set(bob, 50)

	msg1 := msgSend{
		Sender:        alice,
		Recipient:     bob,
		Amount:        10,
		DoReadingCh:   closedChan(),
		ReadingDoneCh: make(chan struct{}, 1),
		DoWritingCh:   make(chan struct{}, 1),
		WritingDoneCh: make(chan struct{}, 1),
	}
	msg2 := msgSend{
		Sender:        alice,
		Recipient:     bob,
		Amount:        10,
		DoReadingCh:   closedChan(),
		ReadingDoneCh: make(chan struct{}, 1),
		DoWritingCh:   make(chan struct{}, 1),
		WritingDoneCh: make(chan struct{}, 1),
	}

	results := do(ctx, requireT, s,
		func() {
			<-msg1.ReadingDoneCh
			<-msg2.ReadingDoneCh
			msg2.DoWritingCh <- struct{}{}
			<-msg2.WritingDoneCh
			msg1.DoWritingCh <- struct{}{}
			<-msg1.WritingDoneCh
			<-msg2.ReadingDoneCh
			msg2.DoWritingCh <- struct{}{}
			<-msg2.WritingDoneCh
		},
		sendHandler(msg1),
		sendHandler(msg2),
	)

	requireT.Equal([]error{nil, nil}, results)

	aliceBalance, exists := s.Get(alice)
	requireT.True(exists)
	bobBalance, exists := s.Get(bob)
	requireT.True(exists)

	requireT.Equal(80, int(aliceBalance))
	requireT.Equal(70, int(bobBalance))
}

func TestTwoTasksFirstWithError(t *testing.T) {
	requireT := require.New(t)
	ctx := newContext(t)

	s := newTestStore()
	s.Set(alice, 100)
	s.Set(bob, 50)

	errTest := errors.New("test err")

	msg1 := msgSend{
		Sender:        alice,
		Recipient:     bob,
		Amount:        5,
		DoReadingCh:   closedChan(),
		ReadingDoneCh: make(chan struct{}, 1),
		DoWritingCh:   closedChan(),
		WritingDoneCh: make(chan struct{}, 1),
		Err:           errTest,
	}
	msg2 := msgSend{
		Sender:        alice,
		Recipient:     bob,
		Amount:        10,
		DoReadingCh:   make(chan struct{}, 1),
		ReadingDoneCh: make(chan struct{}, 1),
		DoWritingCh:   make(chan struct{}, 1),
		WritingDoneCh: make(chan struct{}, 1),
	}

	results := do(ctx, requireT, s,
		func() {
			<-msg1.WritingDoneCh
			msg2.DoReadingCh <- struct{}{}
			msg2.DoWritingCh <- struct{}{}
		},
		sendHandler(msg1),
		sendHandler(msg2),
	)

	requireT.Equal([]error{errTest, nil}, results)

	aliceBalance, exists := s.Get(alice)
	requireT.True(exists)
	bobBalance, exists := s.Get(bob)
	requireT.True(exists)

	requireT.Equal(90, int(aliceBalance))
	requireT.Equal(60, int(bobBalance))
}

func TestTwoTasksSecondWithError(t *testing.T) {
	requireT := require.New(t)
	ctx := newContext(t)

	s := newTestStore()
	s.Set(alice, 100)
	s.Set(bob, 50)

	errTest := errors.New("test err")

	msg1 := msgSend{
		Sender:        alice,
		Recipient:     bob,
		Amount:        5,
		DoReadingCh:   closedChan(),
		ReadingDoneCh: make(chan struct{}, 1),
		DoWritingCh:   make(chan struct{}, 1),
		WritingDoneCh: make(chan struct{}, 1),
	}
	msg2 := msgSend{
		Sender:        alice,
		Recipient:     bob,
		Amount:        10,
		DoReadingCh:   closedChan(),
		ReadingDoneCh: make(chan struct{}, 1),
		DoWritingCh:   make(chan struct{}, 1),
		WritingDoneCh: make(chan struct{}, 1),
		Err:           errTest,
	}

	results := do(ctx, requireT, s,
		func() {
			<-msg1.ReadingDoneCh
			<-msg2.ReadingDoneCh
			msg2.DoWritingCh <- struct{}{}
			<-msg2.WritingDoneCh
			msg1.DoWritingCh <- struct{}{}
			<-msg1.WritingDoneCh
			<-msg2.ReadingDoneCh
			msg2.DoWritingCh <- struct{}{}
			<-msg2.WritingDoneCh
		},
		sendHandler(msg1),
		sendHandler(msg2),
	)

	requireT.Equal([]error{nil, errTest}, results)

	aliceBalance, exists := s.Get(alice)
	requireT.True(exists)
	bobBalance, exists := s.Get(bob)
	requireT.True(exists)

	requireT.Equal(95, int(aliceBalance))
	requireT.Equal(55, int(bobBalance))
}

func TestDeleteWithoutError(t *testing.T) {
	requireT := require.New(t)
	ctx := newContext(t)

	s := newTestStore()
	s.Set(alice, 100)
	s.Set(bob, 50)

	msg := msgSend{
		Sender:        alice,
		Recipient:     bob,
		Amount:        10,
		DoReadingCh:   closedChan(),
		ReadingDoneCh: make(chan struct{}, 1),
		DoWritingCh:   closedChan(),
		WritingDoneCh: make(chan struct{}, 1),
	}

	results := do(ctx, requireT, s,
		nil,
		sendHandler(msg),
		deleteHandler(nil),
	)

	requireT.Equal([]error{nil, nil}, results)

	aliceBalance, exists := s.Get(alice)
	requireT.False(exists)
	bobBalance, exists := s.Get(bob)
	requireT.True(exists)

	requireT.Equal(0, int(aliceBalance))
	requireT.Equal(60, int(bobBalance))
}

func TestDeleteWithError(t *testing.T) {
	requireT := require.New(t)
	ctx := newContext(t)

	s := newTestStore()
	s.Set(alice, 100)
	s.Set(bob, 50)

	errTest := errors.New("test error")

	msg := msgSend{
		Sender:        alice,
		Recipient:     bob,
		Amount:        10,
		DoReadingCh:   closedChan(),
		ReadingDoneCh: make(chan struct{}, 1),
		DoWritingCh:   closedChan(),
		WritingDoneCh: make(chan struct{}, 1),
	}

	results := do(ctx, requireT, s,
		nil,
		sendHandler(msg),
		deleteHandler(errTest),
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
	ctx := newContext(t)

	s := newTestStore()
	s.Set(alice, 100+count*10)
	s.Set(bob, 50)

	handlers := make([]HandlerFunc[string, uint64], 0, count)
	expectedResults := make([]error, count)
	for i := 0; i < count; i++ {
		handlers = append(handlers, sendHandler(msgSend{
			Sender:        alice,
			Recipient:     bob,
			Amount:        10,
			DoReadingCh:   closedChan(),
			ReadingDoneCh: make(chan struct{}, count),
			DoWritingCh:   closedChan(),
			WritingDoneCh: make(chan struct{}, count),
		}))
	}

	results := do(ctx, requireT, s, nil, handlers...)

	requireT.Equal(expectedResults, results)

	aliceBalance, exists := s.Get(alice)
	requireT.True(exists)
	bobBalance, exists := s.Get(bob)
	requireT.True(exists)

	requireT.Equal(100, int(aliceBalance))
	requireT.Equal(50+count*10, int(bobBalance))
}

func do(
	ctx context.Context,
	requireT *require.Assertions,
	s Store[string, uint64],
	managerFunc func(),
	handlers ...HandlerFunc[string, uint64],
) []error {
	results := make([]error, 0, len(handlers))

	requireT.NoError(parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
		handlerCh := make(chan HandlerFunc[string, uint64], len(handlers))
		for _, handler := range handlers {
			handlerCh <- handler
		}
		close(handlerCh)
		resultCh := make(chan error)

		spawn("fusion", parallel.Continue, func(ctx context.Context) error {
			return Run[string, uint64, string](ctx, s, hashingFunc, handlerCh, resultCh)
		})
		spawn("results", parallel.Continue, func(ctx context.Context) error {
			for {
				select {
				case <-ctx.Done():
					return errors.WithStack(ctx.Err())
				case err := <-resultCh:
					results = append(results, err)
					if len(results) == cap(results) {
						return nil
					}
				}
			}
		})
		if managerFunc != nil {
			spawn("manager", parallel.Continue, func(ctx context.Context) error {
				managerFunc()
				return nil
			})
		}

		return nil
	}))

	return results
}
