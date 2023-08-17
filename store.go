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

var errInconsistentRead = errors.New("inconsistent read detected")

type revisionDiffStore[TKey, TValue any, THash comparable] struct {
	parentStore Store[TKey, TValue]
	hashingFunc HashingFunc[TKey, THash]

	mu       sync.RWMutex
	cache    map[THash]revisionDiff[TKey, TValue]
	diffList *list[THash]
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
	hash := s.hashingFunc(key)

	s.mu.RLock()
	defer s.mu.RUnlock()

	if diff, exists := s.cache[hash]; exists {
		return diff.TaskIndex, diff.Value, diff.Exists
	}

	value, exists := s.parentStore.Get(key)
	return 0, value, exists
}

func (s *revisionDiffStore[TKey, TValue, THash]) mergeTaskDiff(
	err error,
	store *taskDiffStore[TKey, TValue, THash],
) error {
	if store.readList != nil {
		for item := store.readList.Head; item != nil; item = item.Next {
			for _, r := range item.Slice {
				if diff := s.cache[r.Hash]; diff.TaskIndex > r.TaskIndex {
					return errInconsistentRead
				}
			}
		}
	}

	if err != nil {
		return nil //nolint:nilerr
	}

	s.mu.Lock()
	defer s.mu.Unlock()

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
	diffList *list[THash],
	readList *list[readRevision[THash]],
) *taskDiffStore[TKey, TValue, THash] {
	diffList.Reset()
	readList.Reset()

	return &taskDiffStore[TKey, TValue, THash]{
		taskIndex:   taskIndex,
		parentStore: parentStore,
		hashingFunc: hashingFunc,
		cache:       map[THash]taskDiff[TKey, TValue]{},
		diffList:    diffList,
		readList:    readList,
	}
}

func (s *taskDiffStore[TKey, TValue, THash]) Key(key TKey) KeyStore[TKey, TValue, THash] {
	return newKeyStore[TKey, TValue, THash](s, key, s.hashingFunc(key))
}

func (s *taskDiffStore[TKey, TValue, THash]) get(key TKey, hash THash) (TValue, bool) {
	if diff, exists := s.cache[hash]; exists {
		return diff.Value, diff.Exists
	}

	taskIndex, value, exists := s.parentStore.Get(key)

	if s.readList != nil {
		s.readList.Append(readRevision[THash]{
			TaskIndex: taskIndex,
			Hash:      hash,
		})
	}
	s.cache[hash] = taskDiff[TKey, TValue]{
		Key:    key,
		Value:  value,
		Exists: exists,
	}
	return value, exists
}

func (s *taskDiffStore[TKey, TValue, THash]) set(key TKey, hash THash, value TValue) {
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

func (s *taskDiffStore[TKey, TValue, THash]) delete(key TKey, hash THash) {
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

func (s *taskDiffStore[TKey, TValue, THash]) Reset() {
	s.cache = map[THash]taskDiff[TKey, TValue]{}
	s.readList = nil
	s.diffList.Reset()
}

// KeyStore is the store responsible for getting and storing value of single key.
type KeyStore[TKey, TValue any, THash comparable] struct {
	store *taskDiffStore[TKey, TValue, THash]
	key   TKey
	hash  THash
}

func newKeyStore[TKey, TValue any, THash comparable](
	store *taskDiffStore[TKey, TValue, THash],
	key TKey,
	hash THash,
) KeyStore[TKey, TValue, THash] {
	return KeyStore[TKey, TValue, THash]{
		store: store,
		key:   key,
		hash:  hash,
	}
}

// Get returns the value.
func (ks KeyStore[TKey, TValue, THash]) Get() (TValue, bool) {
	return ks.store.get(ks.key, ks.hash)
}

// Set sets the value.
func (ks KeyStore[TKey, TValue, THash]) Set(value TValue) {
	ks.store.set(ks.key, ks.hash, value)
}

// Delete deletes the key.
func (ks KeyStore[TKey, TValue, THash]) Delete() {
	ks.store.delete(ks.key, ks.hash)
}
