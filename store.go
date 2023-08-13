package fusion

import (
	"sync"

	"github.com/pkg/errors"
)

type revisionDiff[TKey, TValue any] struct {
	TaskIndex uint64
	Key       TKey
	Value     TValue
	Deleted   bool
}

type taskDiff[TKey, TValue any] struct {
	Key     TKey
	Value   TValue
	Deleted bool
}

type keyValue[TKey, TValue any] struct {
	TaskIndex uint64
	Key       TKey
	Value     TValue
	Exists    bool
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
	diffHashes []THash
}

func newRevisionDiffStore[TKey, TValue any, THash comparable](
	parentStore Store[TKey, TValue],
	hashingFunc HashingFunc[TKey, THash],
) *revisionDiffStore[TKey, TValue, THash] {
	return &revisionDiffStore[TKey, TValue, THash]{
		parentStore: parentStore,
		hashingFunc: hashingFunc,
		diff:        map[THash]revisionDiff[TKey, TValue]{},
	}
}

func (s *revisionDiffStore[TKey, TValue, THash]) Get(key TKey) (uint64, TValue, bool) {
	s.mu.RLock()
	hash := s.hashingFunc(key)
	if diff, exists := s.diff[hash]; exists {
		defer s.mu.RUnlock()
		return diff.TaskIndex, diff.Value, !diff.Deleted
	}
	s.mu.RUnlock()

	value, exists := s.parentStore.Get(key)
	return 0, value, exists
}

func (s *revisionDiffStore[TKey, TValue, THash]) mergeTaskDiff(err error, store *taskDiffStore[TKey, TValue, THash]) error {
	s.mu.RLock()

	for hash, read := range store.get {
		if diff, exists := s.diff[hash]; exists {
			if diff.TaskIndex > read.TaskIndex {
				s.mu.RUnlock()
				return errInconsistentRead
			}
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
		return nil
	}

	for hash, diff := range store.diff {
		if _, exists := s.diff[hash]; !exists {
			s.diffHashes = append(s.diffHashes, hash)
		}
		s.diff[hash] = revisionDiff[TKey, TValue]{
			TaskIndex: store.taskIndex,
			Key:       diff.Key,
			Value:     diff.Value,
			Deleted:   diff.Deleted,
		}
	}

	return nil
}

func (s *revisionDiffStore[TKey, TValue, THash]) applyTo(store Store[TKey, TValue]) {
	for _, hash := range s.diffHashes {
		diff := s.diff[hash]
		if diff.Deleted {
			store.Delete(diff.Key)
		} else {
			store.Set(diff.Key, diff.Value)
		}
	}
}

type taskDiffStore[TKey, TValue any, THash comparable] struct {
	taskIndex   uint64
	parentStore *revisionDiffStore[TKey, TValue, THash]
	hashingFunc HashingFunc[TKey, THash]
	get         map[THash]keyValue[TKey, TValue]
	diff        map[THash]taskDiff[TKey, TValue]
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
		get:         map[THash]keyValue[TKey, TValue]{},
		diff:        map[THash]taskDiff[TKey, TValue]{},
	}
}

func (s *taskDiffStore[TKey, TValue, THash]) Get(key TKey) (TValue, bool) {
	hash := s.hashingFunc(key)
	if diff, exists := s.diff[hash]; exists {
		return diff.Value, !diff.Deleted
	}
	if kv, exists := s.get[hash]; exists {
		return kv.Value, kv.Exists
	}

	taskIndex, value, exists := s.parentStore.Get(key)
	s.get[hash] = keyValue[TKey, TValue]{
		TaskIndex: taskIndex,
		Key:       key,
		Value:     value,
		Exists:    exists,
	}
	return value, exists
}

func (s *taskDiffStore[TKey, TValue, THash]) Set(key TKey, value TValue) {
	s.diff[s.hashingFunc(key)] = taskDiff[TKey, TValue]{
		Key:   key,
		Value: value,
	}
}

func (s *taskDiffStore[TKey, TValue, THash]) Delete(key TKey) {
	s.diff[s.hashingFunc(key)] = taskDiff[TKey, TValue]{
		Key:     key,
		Deleted: true,
	}
}
