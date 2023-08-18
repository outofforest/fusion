package fusion

import (
	"github.com/pkg/errors"
)

// Run executes handlers.
func Run[TKey, TValue any, THash comparable](
	store Store[TKey, TValue],
	hashingFunc HashingFunc[TKey, THash],
	nHandlers int,
	handlerCh <-chan HandlerFunc[TKey, TValue, THash],
) ([]error, error) {
	revisionStore := newRevisionDiffStore[TKey, TValue, THash](store, hashingFunc)
	taskStore := newTaskDiffStore[TKey, TValue, THash](
		revisionStore,
		hashingFunc,
		newList[THash](),
	)
	results := make([]error, 0, nHandlers)

	for handlerFunc := range handlerCh {
		taskStore.Reset()

		res := runHandler(handlerFunc, taskStore)
		results = append(results, res)
		if res == nil {
			revisionStore.mergeDiff(taskStore)
		}
	}

	revisionStore.applyTo(store)
	return results, nil
}

func runHandler[TKey, TValue any, THash comparable](
	handlerFunc HandlerFunc[TKey, TValue, THash],
	store *taskDiffStore[TKey, TValue, THash],
) (retErr error) {
	defer func() {
		if r := recover(); r != nil {
			if err, ok := r.(error); ok {
				retErr = err
			} else {
				retErr = errors.Errorf("panic: %s", r)
			}
		}
	}()
	return handlerFunc(store)
}
