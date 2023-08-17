package fusion

import (
	"context"
	"fmt"
	"sync"

	"github.com/outofforest/parallel"
	"github.com/pkg/errors"
)

const nWorkers = 5

type task[TKey, TValue any, THash comparable] struct {
	TaskIndex       uint64
	HandlerFunc     HandlerFunc[TKey, TValue, THash]
	PrevTaskReadyCh <-chan struct{}
	MergeNextTaskCh chan<- struct{}
}

type state[TKey, TValue any, THash comparable] struct {
	results []error

	mu              sync.Mutex
	taskIndex       uint64
	handlerCh       <-chan HandlerFunc[TKey, TValue, THash]
	prevTaskReadyCh chan struct{}
	mergeNextTaskCh chan struct{}
}

func newState[TKey, TValue any, THash comparable](
	nHandlers int,
	handlerCh <-chan HandlerFunc[TKey, TValue, THash],
) *state[TKey, TValue, THash] {
	prevTaskReadyCh := make(chan struct{})
	close(prevTaskReadyCh)
	return &state[TKey, TValue, THash]{
		results:         make([]error, 0, nHandlers),
		handlerCh:       handlerCh,
		prevTaskReadyCh: prevTaskReadyCh,
		mergeNextTaskCh: make(chan struct{}, 1),
	}
}

func (s *state[TKey, TValue, THash]) Next(ctx context.Context) (task[TKey, TValue, THash], bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	select {
	case <-ctx.Done():
		return task[TKey, TValue, THash]{}, false, errors.WithStack(ctx.Err())
	case handlerFunc, ok := <-s.handlerCh:
		if !ok {
			return task[TKey, TValue, THash]{}, false, nil
		}

		s.taskIndex++
		t := task[TKey, TValue, THash]{
			TaskIndex:       s.taskIndex,
			HandlerFunc:     handlerFunc,
			PrevTaskReadyCh: s.prevTaskReadyCh,
			MergeNextTaskCh: s.mergeNextTaskCh,
		}

		s.prevTaskReadyCh = s.mergeNextTaskCh
		s.mergeNextTaskCh = make(chan struct{}, 1)

		return t, true, nil
	}
}

// Run executes handlers.
func Run[TKey, TValue any, THash comparable](
	ctx context.Context,
	store Store[TKey, TValue],
	hashingFunc HashingFunc[TKey, THash],
	nHandlers int,
	handlerCh <-chan HandlerFunc[TKey, TValue, THash],
) ([]error, error) {
	revisionStore := newRevisionDiffStore[TKey, TValue, THash](store, hashingFunc)
	state := newState[TKey, TValue, THash](nHandlers, handlerCh)

	err := parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
		for i := 0; i < nWorkers; i++ {
			spawn(fmt.Sprintf("worker-%d", i), parallel.Continue, func(ctx context.Context) error {
				return worker[TKey, TValue, THash](ctx, hashingFunc, state, revisionStore)
			})
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	revisionStore.applyTo(store)

	// returning ctx.Err() is important to not save inconsistent results in case execution has been canceled
	return state.results, errors.WithStack(ctx.Err())
}

func worker[TKey, TValue any, THash comparable](
	ctx context.Context,
	hashingFunc HashingFunc[TKey, THash],
	state *state[TKey, TValue, THash],
	revisionStore *revisionDiffStore[TKey, TValue, THash],
) error {
	diffList := newList[THash]()
	readList := newList[readRevision[THash]]()

mainLoop:
	for {
		task, ok, err := state.Next(ctx)
		if !ok || err != nil {
			return err
		}

		taskStore := newTaskDiffStore[TKey, TValue, THash](
			task.TaskIndex,
			revisionStore,
			hashingFunc,
			diffList,
			readList,
		)

		for {
			var errHandler error
			func() {
				defer func() {
					if r := recover(); r != nil {
						if err, ok := r.(error); ok {
							errHandler = err
						} else {
							errHandler = errors.Errorf("panic: %s", r)
						}
					}
				}()
				errHandler = task.HandlerFunc(ctx, taskStore)
			}()

			<-task.PrevTaskReadyCh

			err := revisionStore.mergeTaskDiff(errHandler, taskStore)
			switch err {
			case errInconsistentRead:
				taskStore.Reset()
				continue
			default:
				state.results = append(state.results, errHandler)
				close(task.MergeNextTaskCh)
				continue mainLoop
			}
		}
	}
}
