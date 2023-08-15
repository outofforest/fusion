package fusion

import (
	"context"
	"fmt"

	"github.com/outofforest/parallel"
	"github.com/pkg/errors"
)

const nWorkers = 5

type task[TKey, TValue any, THash comparable] struct {
	TaskIndex       uint64
	HandlerFunc     HandlerFunc[TKey, TValue]
	PrevTaskReadyCh <-chan struct{}
	TaskReadyCh     chan<- struct{}
}

// Run executes handlers.
func Run[TKey, TValue any, THash comparable](
	ctx context.Context,
	store Store[TKey, TValue],
	hashingFunc HashingFunc[TKey, THash],
	handlerCh <-chan HandlerFunc[TKey, TValue],
	resultCh chan<- error,
) error {
	defer close(resultCh)

	revisionStore := newRevisionDiffStore[TKey, TValue, THash](store, hashingFunc)

	err := parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
		availableWorkerCh := make(chan struct{}, nWorkers)
		for i := 0; i < nWorkers; i++ {
			availableWorkerCh <- struct{}{}
		}
		taskCh := make(chan task[TKey, TValue, THash], nWorkers)

		spawn("taskDistributor", parallel.Exit, func(ctx context.Context) error {
			return taskDistributor[TKey, TValue, THash](ctx, handlerCh, taskCh, availableWorkerCh)
		})
		for i := 0; i < nWorkers; i++ {
			spawn(fmt.Sprintf("worker-%d", i), parallel.Continue, func(ctx context.Context) error {
				return worker[TKey, TValue, THash](ctx, hashingFunc, taskCh, resultCh, availableWorkerCh, revisionStore)
			})
		}

		return nil
	})
	if err != nil {
		return err
	}

	revisionStore.applyTo(store)

	// returning ctx.Err() is important to not save inconsistent results in case execution has been canceled
	return errors.WithStack(ctx.Err())
}

func taskDistributor[TKey, TValue any, THash comparable](
	ctx context.Context,
	handlerCh <-chan HandlerFunc[TKey, TValue],
	taskCh chan<- task[TKey, TValue, THash],
	availableWorkerCh chan struct{},
) error {
	defer func() {
		close(taskCh)

		// -1 is here because one item is always taken from `availableWorkerCh` in the main `for` loop below.
		for i := 0; i < nWorkers-1; i++ {
			<-availableWorkerCh
		}
	}()

	var handlerFunc HandlerFunc[TKey, TValue]
	var ok bool

	prevTaskReadyCh := make(chan struct{})
	close(prevTaskReadyCh)
	taskReadyCh := make(chan struct{}, 1)

	// 1 is here because 0 means that value was read from persistent store
	for taskIndex := uint64(1); ; taskIndex++ {
		<-availableWorkerCh

		select {
		case <-ctx.Done():
			return errors.WithStack(ctx.Err())
		case handlerFunc, ok = <-handlerCh:
			if !ok {
				return errors.WithStack(ctx.Err())
			}
		}

		taskCh <- task[TKey, TValue, THash]{
			TaskIndex:       taskIndex,
			HandlerFunc:     handlerFunc,
			PrevTaskReadyCh: prevTaskReadyCh,
			TaskReadyCh:     taskReadyCh,
		}

		prevTaskReadyCh = taskReadyCh
		taskReadyCh = make(chan struct{}, 1)
	}
}

func worker[TKey, TValue any, THash comparable](
	ctx context.Context,
	hashingFunc HashingFunc[TKey, THash],
	taskCh <-chan task[TKey, TValue, THash],
	resultCh chan<- error,
	availableWorkerCh chan<- struct{},
	revisionStore *revisionDiffStore[TKey, TValue, THash],
) error {
loop:
	for task := range taskCh {
	taskLoop:
		for {
			taskStore := newTaskDiffStore[TKey, TValue, THash](task.TaskIndex, revisionStore, hashingFunc)
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

			for {
				err := revisionStore.mergeTaskDiff(errHandler, taskStore)
				switch {
				case err == nil:
					select {
					case task.TaskReadyCh <- struct{}{}:
					default:
					}

					resultCh <- errHandler
					availableWorkerCh <- struct{}{}
					continue loop
				case errors.Is(err, errInconsistentRead):
					continue taskLoop
				case errors.Is(err, errAwaitingPreviousTask):
					<-task.PrevTaskReadyCh

					select {
					case task.TaskReadyCh <- struct{}{}:
					default:
					}
				}
			}
		}
	}

	return nil
}
