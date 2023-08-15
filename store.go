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
	Key     TKey
	Value   TValue
	Exists  bool
	Updated bool
}

type readRevision[THash comparable] struct {
	TaskIndex uint64
	Hash      THash
}

var (
	errInconsistentRead     = errors.New("inconsistent read detected")
	errAwaitingPreviousTask = errors.New("await previous task")
)

type revisionDiffStore[TKey, TValue any, THash comparable] struct {
	parentStore Store[TKey, TValue]
	hashingFunc HashingFunc[TKey, THash]

	mu        sync.RWMutex
	taskIndex uint64
	cache     map[THash]revisionDiff[TKey, TValue]
	diffList  *list[THash]
}

func newRevisionDiffStore[TKey, TValue any, THash comparable](
	parentStore Store[TKey, TValue],
	hashingFunc HashingFunc[TKey, THash],
) *revisionDiffStore[TKey, TValue, THash] {
	return &revisionDiffStore[TKey, TValue, THash]{
		parentStore: parentStore,
		hashingFunc: hashingFunc,
		cache:       map[THash]revisionDiff[TKey, TValue]{},
		diffList:    newList[THash](),
	}
}

func (s *revisionDiffStore[TKey, TValue, THash]) Get(key TKey) (uint64, TValue, bool) {
	s.mu.RLock()
	hash := s.hashingFunc(key)
	if diff, exists := s.cache[hash]; exists {
		defer s.mu.RUnlock()
		return diff.TaskIndex, diff.Value, diff.Exists
	}
	s.mu.RUnlock()

	value, exists := s.parentStore.Get(key)
	return 0, value, exists
}

func (s *revisionDiffStore[TKey, TValue, THash]) mergeTaskDiff(err error, store *taskDiffStore[TKey, TValue, THash]) error {
	s.mu.RLock()
	for item := store.readList.Head; item != nil; item = item.Next {
		for _, r := range item.Slice {
			if diff := s.cache[r.Hash]; diff.TaskIndex > r.TaskIndex {
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
		return nil //nolint:nilerr
	}

	for item := store.diffList.Head; item != nil; item = item.Next {
		for _, hash := range item.Slice {
			if _, exists := s.cache[hash]; !exists {
				s.diffList.Append(hash)
			}
			diff := store.cache[hash]
			s.cache[hash] = revisionDiff[TKey, TValue]{
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
	for item := s.diffList.Head; item != nil; item = item.Next {
		for _, hash := range item.Slice {
			diff := s.cache[hash]
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
	cache       map[THash]taskDiff[TKey, TValue]
	diffList    *list[THash]
	readList    *list[readRevision[THash]]
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
		cache:       map[THash]taskDiff[TKey, TValue]{},
		diffList:    newList[THash](),
		readList:    newList[readRevision[THash]](),
	}
}

func (s *taskDiffStore[TKey, TValue, THash]) Get(key TKey) (TValue, bool) {
	hash := s.hashingFunc(key)
	if diff, exists := s.cache[hash]; exists {
		return diff.Value, diff.Exists
	}

	taskIndex, value, exists := s.parentStore.Get(key)
	s.readList.Append(readRevision[THash]{
		TaskIndex: taskIndex,
		Hash:      hash,
	})
	s.cache[hash] = taskDiff[TKey, TValue]{
		Key:    key,
		Value:  value,
		Exists: exists,
	}
	return value, exists
}

func (s *taskDiffStore[TKey, TValue, THash]) Set(key TKey, value TValue) {
	hash := s.hashingFunc(key)
	diff, exists := s.cache[hash]
	if exists {
		diff.Value = value
		diff.Exists = true
	} else {
		diff = taskDiff[TKey, TValue]{
			Key:    key,
			Value:  value,
			Exists: true,
		}
	}
	if !diff.Updated {
		diff.Updated = true
		s.diffList.Append(hash)
	}
	s.cache[hash] = diff
}

func (s *taskDiffStore[TKey, TValue, THash]) Delete(key TKey) {
	hash := s.hashingFunc(key)
	diff, exists := s.cache[hash]
	if exists {
		var v TValue
		diff.Value = v
		diff.Exists = false
	} else {
		diff = taskDiff[TKey, TValue]{
			Key: key,
		}
	}
	if !diff.Updated {
		diff.Updated = true
		s.diffList.Append(hash)
	}
	s.cache[hash] = diff
}
