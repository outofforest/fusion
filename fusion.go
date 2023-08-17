package fusion

import (
	"context"
	"fmt"
	"sync"

	"github.com/outofforest/parallel"
	"github.com/pkg/errors"
)

const nWorkers = 2

type task[TKey, TValue any, THash comparable] struct {
	TaskIndex   uint64
	HandlerFunc HandlerFunc[TKey, TValue, THash]
	Store       *taskDiffStore[TKey, TValue, THash]
}

type result[TKey, TValue any, THash comparable] struct {
	TaskIndex uint64
	Store     *taskDiffStore[TKey, TValue, THash]
	Result    error
}

type state[TKey, TValue any, THash comparable] struct {
	taskStores []*taskDiffStore[TKey, TValue, THash]
	results    []error

	mu            sync.Mutex
	taskIndex     uint64
	handlerCh     <-chan HandlerFunc[TKey, TValue, THash]
	resultCh      chan result[TKey, TValue, THash]
	revisionStore *revisionDiffStore[TKey, TValue, THash]
	diffList      *list[THash]
	cache         map[THash]revisionDiff[TKey, TValue]
	lockedKeys    map[THash]struct{}
}

func newState[TKey, TValue any, THash comparable](
	nHandlers int,
	handlerCh <-chan HandlerFunc[TKey, TValue, THash],
	hashingFunc HashingFunc[TKey, THash],
	revisionStore *revisionDiffStore[TKey, TValue, THash],
) *state[TKey, TValue, THash] {
	taskStores := make([]*taskDiffStore[TKey, TValue, THash], 0, nWorkers)
	for i := 0; i < nWorkers; i++ {
		taskStores = append(taskStores, newTaskDiffStore[TKey, TValue, THash](
			revisionStore,
			hashingFunc,
			newList[THash](),
			newList[THash](),
		))
	}
	return &state[TKey, TValue, THash]{
		taskStores:    taskStores,
		results:       make([]error, nHandlers),
		handlerCh:     handlerCh,
		resultCh:      make(chan result[TKey, TValue, THash], nWorkers),
		revisionStore: revisionStore,
		diffList:      newList[THash](),
		cache:         map[THash]revisionDiff[TKey, TValue]{},
		lockedKeys:    map[THash]struct{}{},
	}
}

func (s *state[TKey, TValue, THash]) Next(ctx context.Context) (task[TKey, TValue, THash], bool, error) {
	s.mu.Lock()

	merge := func(res result[TKey, TValue, THash]) {
		s.results[res.TaskIndex] = res.Result
		if res.Result == nil {
			taskCache := res.Store.cache
			for item := res.Store.diffListSealed.Head; item != nil; item = item.Next {
				for _, hash := range item.Slice {
					if _, exists := s.cache[hash]; !exists {
						s.diffList.Append(hash)
					}
					diff := taskCache[hash]
					s.cache[hash] = revisionDiff[TKey, TValue](diff)
				}
			}
		}
	}

	waitTasks := func(n int) {
		for i := n; i > 0; i-- {
			res := <-s.resultCh
			merge(res)
			s.taskStores = append(s.taskStores, res.Store)
		}
	}

loop:
	for {
		select {
		case <-ctx.Done():
			s.mu.Unlock()
			return task[TKey, TValue, THash]{}, false, errors.WithStack(ctx.Err())
		case res := <-s.resultCh:
			merge(res)
			s.taskStores = append(s.taskStores, res.Store)
		case handlerFunc, ok := <-s.handlerCh:
			if !ok {
				break loop
			}

			var taskStore *taskDiffStore[TKey, TValue, THash]
			l := len(s.taskStores) - 1
			if l == -1 {
				res := <-s.resultCh
				merge(res)
				taskStore = res.Store
			} else {
				taskStore = s.taskStores[l]
				s.taskStores = s.taskStores[:l]
			}

			t := task[TKey, TValue, THash]{
				TaskIndex:   s.taskIndex,
				HandlerFunc: handlerFunc,
				Store:       taskStore,
			}
			s.taskIndex++

			taskStore.Reset(func(keyList *list[THash]) {
				var conflict bool
			keyLoop:
				for item := keyList.Head; item != nil; item = item.Next {
					for _, hash := range item.Slice {
						if _, exists := s.lockedKeys[hash]; exists {
							conflict = true
							break keyLoop
						} else {
							s.lockedKeys[hash] = struct{}{}
						}
					}
				}
				if conflict {
					waitTasks(nWorkers - len(s.taskStores) - 1)
					s.revisionStore.mergeDiff(s.diffList, s.cache)
					s.diffList.Reset()
					s.cache = map[THash]revisionDiff[TKey, TValue]{}
					s.lockedKeys = map[THash]struct{}{}
					for item := keyList.Head; item != nil; item = item.Next {
						for _, hash := range item.Slice {
							s.lockedKeys[hash] = struct{}{}
						}
					}
				}

				s.mu.Unlock()
			})

			return t, true, nil
		}
	}

	waitTasks(nWorkers - len(s.taskStores))
	s.revisionStore.mergeDiff(s.diffList, s.cache)

	s.mu.Unlock()

	return task[TKey, TValue, THash]{}, false, nil
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
	state := newState[TKey, TValue, THash](nHandlers, handlerCh, hashingFunc, revisionStore)

	err := parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
		for i := 0; i < nWorkers; i++ {
			spawn(fmt.Sprintf("worker-%d", i), parallel.Continue, func(ctx context.Context) error {
				return worker[TKey, TValue, THash](ctx, state)
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
	state *state[TKey, TValue, THash],
) error {
	for {
		task, ok, err := state.Next(ctx)
		if !ok || err != nil {
			return err
		}

		var res error
		func() {
			defer func() {
				if r := recover(); r != nil {
					if err, ok := r.(error); ok {
						res = err
					} else {
						res = errors.Errorf("panic: %s", r)
					}
				}
			}()
			res = task.HandlerFunc(ctx, task.Store)
		}()

		state.resultCh <- result[TKey, TValue, THash]{
			TaskIndex: task.TaskIndex,
			Store:     task.Store,
			Result:    res,
		}
	}
}
