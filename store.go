package fusion

import (
	"sync"

	"github.com/pkg/errors"
)

type revisionDiff[TKey, TValue any] struct {
	TaskIndex uint64
	Key       TKey
	Value     TValue
	Exists    bool
}

type taskDiff[TKey, TValue any] struct {
	TaskIndex uint64
	Key       TKey
	Value     TValue
	Exists    bool
	Updated   bool
}

var (
	errInconsistentRead     = errors.New("inconsistent read detected")
	errAwaitingPreviousTask = errors.New("await previous task")
)

type revisionDiffStore[TKey, TValue any, THash comparable] struct {
	parentStore Store[TKey, TValue]
	hashingFunc HashingFunc[TKey, THash]

	mu         sync.RWMutex
	taskIndex  uint64
	diff       map[THash]revisionDiff[TKey, TValue]
	diffHashes *list[THash]
}

func newRevisionDiffStore[TKey, TValue any, THash comparable](
	parentStore Store[TKey, TValue],
	hashingFunc HashingFunc[TKey, THash],
) *revisionDiffStore[TKey, TValue, THash] {
	return &revisionDiffStore[TKey, TValue, THash]{
		parentStore: parentStore,
		hashingFunc: hashingFunc,
		diff:        map[THash]revisionDiff[TKey, TValue]{},
		diffHashes:  newList[THash](),
	}
}

func (s *revisionDiffStore[TKey, TValue, THash]) Get(key TKey) (uint64, TValue, bool) {
	s.mu.RLock()
	hash := s.hashingFunc(key)
	if diff, exists := s.diff[hash]; exists {
		defer s.mu.RUnlock()
		return diff.TaskIndex, diff.Value, diff.Exists
	}
	s.mu.RUnlock()

	value, exists := s.parentStore.Get(key)
	return 0, value, exists
}

func (s *revisionDiffStore[TKey, TValue, THash]) mergeTaskDiff(err error, store *taskDiffStore[TKey, TValue, THash]) error {
	s.mu.RLock()

	for hash, read := range store.diff {
		if diff := s.diff[hash]; diff.TaskIndex > read.TaskIndex {
			s.mu.RUnlock()
			return errInconsistentRead
		}
	}

	if s.taskIndex != store.taskIndex-1 {
		s.mu.RUnlock()
		return errAwaitingPreviousTask
	}

	s.mu.RUnlock()

	s.mu.Lock()
	defer s.mu.Unlock()

	s.taskIndex = store.taskIndex

	if err != nil {
		return nil //nolint:nilerr
	}

	for item := store.diffHashes.Tail; item != nil; item = item.Previous {
		for _, hash := range item.Slice {
			if _, exists := s.diff[hash]; !exists {
				s.diffHashes.Append(hash)
			}
			diff := store.diff[hash]
			s.diff[hash] = revisionDiff[TKey, TValue]{
				TaskIndex: store.taskIndex,
				Key:       diff.Key,
				Value:     diff.Value,
				Exists:    diff.Exists,
			}
		}
	}

	return nil
}

func (s *revisionDiffStore[TKey, TValue, THash]) applyTo(store Store[TKey, TValue]) {
	for item := s.diffHashes.Tail; item != nil; item = item.Previous {
		for _, hash := range item.Slice {
			diff := s.diff[hash]
			if diff.Exists {
				store.Set(diff.Key, diff.Value)
			} else {
				store.Delete(diff.Key)
			}
		}
	}
}

type taskDiffStore[TKey, TValue any, THash comparable] struct {
	taskIndex   uint64
	parentStore *revisionDiffStore[TKey, TValue, THash]
	hashingFunc HashingFunc[TKey, THash]
	diff        map[THash]taskDiff[TKey, TValue]
	diffHashes  *list[THash]
}

func newTaskDiffStore[TKey, TValue any, THash comparable](
	taskIndex uint64,
	parentStore *revisionDiffStore[TKey, TValue, THash],
	hashingFunc HashingFunc[TKey, THash],
) *taskDiffStore[TKey, TValue, THash] {
	return &taskDiffStore[TKey, TValue, THash]{
		taskIndex:   taskIndex,
		parentStore: parentStore,
		hashingFunc: hashingFunc,
		diff:        map[THash]taskDiff[TKey, TValue]{},
		diffHashes:  newList[THash](),
	}
}

func (s *taskDiffStore[TKey, TValue, THash]) Get(key TKey) (TValue, bool) {
	hash := s.hashingFunc(key)
	if diff, exists := s.diff[hash]; exists {
		return diff.Value, diff.Exists
	}

	taskIndex, value, exists := s.parentStore.Get(key)
	s.diff[hash] = taskDiff[TKey, TValue]{
		TaskIndex: taskIndex,
		Key:       key,
		Value:     value,
		Exists:    exists,
	}
	return value, exists
}

func (s *taskDiffStore[TKey, TValue, THash]) Set(key TKey, value TValue) {
	hash := s.hashingFunc(key)
	diff, exists := s.diff[hash]
	if exists {
		diff.Value = value
		diff.Exists = true
	} else {
		diff = taskDiff[TKey, TValue]{
			TaskIndex: s.taskIndex,
			Key:       key,
			Value:     value,
			Exists:    true,
		}
	}
	if !diff.Updated {
		diff.Updated = true
		s.diffHashes.Append(hash)
	}
	s.diff[hash] = diff
}

func (s *taskDiffStore[TKey, TValue, THash]) Delete(key TKey) {
	hash := s.hashingFunc(key)
	diff, exists := s.diff[hash]
	if exists {
		var v TValue
		diff.Value = v
		diff.Exists = false
	} else {
		diff = taskDiff[TKey, TValue]{
			TaskIndex: s.taskIndex,
			Key:       key,
		}
	}
	if !diff.Updated {
		diff.Updated = true
		s.diffHashes.Append(hash)
	}
	s.diff[hash] = diff
}
