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
	PrevTaskReadyCh <-chan uint64
	TaskReadyCh     chan uint64
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

	return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
		availableWorkerCh := make(chan struct{}, nWorkers)
		for i := 0; i < nWorkers; i++ {
			availableWorkerCh <- struct{}{}
		}
		taskCh := make(chan task[TKey, TValue, THash], nWorkers)

		revisionStore := newRevisionDiffStore[TKey, TValue, THash](store, hashingFunc)

		spawn("taskDistributor", parallel.Exit, func(ctx context.Context) error {
			defer func() {
				close(taskCh)

				// -1 is here because one item is always taken from `availableWorkerCh` in the main `for` loop below.
				for i := 0; i < nWorkers-1; i++ {
					<-availableWorkerCh
				}
			}()

			var handlerFunc HandlerFunc[TKey, TValue]
			var ok bool

			prevTaskReadyCh := make(chan uint64)
			close(prevTaskReadyCh)
			taskReadyCh := make(chan uint64, 1)

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
				taskReadyCh = make(chan uint64, 1)
			}
		})
		for i := 0; i < nWorkers; i++ {
			spawn(fmt.Sprintf("worker-%d", i), parallel.Continue, func(ctx context.Context) error {
				for task := range taskCh {
				taskLoop:
					for {
						taskStore := newTaskDiffStore[TKey, TValue, THash](task.TaskIndex, revisionStore, hashingFunc)

						var err error
						func() {
							defer func() {
								if r := recover(); r != nil {
									err = PanicError{Value: r}
								}
							}()
							err = task.HandlerFunc(ctx, taskStore)
						}()

						if err != nil {
							select {
							case <-task.TaskReadyCh:
							default:
							}
							task.TaskReadyCh <- task.TaskIndex

							resultCh <- err
							availableWorkerCh <- struct{}{}
							return errors.WithStack(ctx.Err())
						}

						for {
							err := revisionStore.applyDiff(taskStore)
							switch {
							case err == nil:
								select {
								case <-task.TaskReadyCh:
								default:
								}
								task.TaskReadyCh <- task.TaskIndex

								resultCh <- nil
								availableWorkerCh <- struct{}{}
								return errors.WithStack(ctx.Err())
							case errors.Is(err, errInconsistentRead):
								continue taskLoop
							case errors.Is(err, errAwaitingPreviousTask):
								indexReady := <-task.PrevTaskReadyCh

								select {
								case <-task.TaskReadyCh:
								default:
								}
								task.TaskReadyCh <- indexReady
							}
						}
					}
				}

				return nil
			})
		}

		return nil
	})
}
