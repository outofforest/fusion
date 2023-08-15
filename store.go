package fusion

import (
	"sync"
)

type revisionDiff[TKey, TValue any] struct {
	Key     TKey
	Value   TValue
	Exists  bool
	Updated bool
	Events  map[chan<- struct{}]uint64
}

type taskDiff[TKey, TValue any] struct {
	Key     TKey
	Value   TValue
	Exists  bool
	Updated bool
}

type revisionDiffStore[TKey, TValue any, THash comparable] struct {
	parentStore Store[TKey, TValue]
	hashingFunc HashingFunc[TKey, THash]

	mu        sync.Mutex
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

func (s *revisionDiffStore[TKey, TValue, THash]) Get(eventCh chan<- struct{}, key TKey) (TValue, bool) {
	hash := s.hashingFunc(key)

	s.mu.Lock()
	defer s.mu.Unlock()

	diff, exists := s.cache[hash]
	if !exists {
		value, exists := s.parentStore.Get(key)
		diff = revisionDiff[TKey, TValue]{
			Key:    key,
			Value:  value,
			Exists: exists,
			Events: map[chan<- struct{}]uint64{
				eventCh: s.taskIndex,
			},
		}
		s.cache[hash] = diff
	} else if _, exists := diff.Events[eventCh]; !exists {
		diff.Events[eventCh] = s.taskIndex
	}

	return diff.Value, diff.Exists
}

func (s *revisionDiffStore[TKey, TValue, THash]) mergeTaskDiff(store *taskDiffStore[TKey, TValue, THash]) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.taskIndex = store.taskIndex

	for item := store.readList.Head; item != nil; item = item.Next {
		for _, hash := range item.Slice {
			delete(s.cache[hash].Events, store.eventCh)
		}
	}

	sentEvents := map[chan<- struct{}]struct{}{}
	for item := store.diffList.Head; item != nil; item = item.Next {
		for _, hash := range item.Slice {
			diff, exists := s.cache[hash]
			diffNew := store.cache[hash]
			if exists {
				diff.Value = diffNew.Value
				diff.Exists = diffNew.Exists

				if !diff.Updated {
					s.diffList.Append(hash)
					diff.Updated = true
				}

				s.cache[hash] = diff

				for eventCh, taskIndex := range diff.Events {
					if store.taskIndex > taskIndex {
						if _, exists := sentEvents[eventCh]; !exists {
							select {
							case eventCh <- struct{}{}:
							default:
							}

							sentEvents[eventCh] = struct{}{}
						}
						delete(diff.Events, eventCh)
					}
				}
			} else {
				s.diffList.Append(hash)
				s.cache[hash] = revisionDiff[TKey, TValue]{
					Key:    diffNew.Key,
					Value:  diffNew.Value,
					Exists: diffNew.Exists,
					Events: map[chan<- struct{}]uint64{},
				}
			}
		}
	}
}

func (s *revisionDiffStore[TKey, TValue, THash]) cleanEvents(store *taskDiffStore[TKey, TValue, THash]) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for item := store.readList.Head; item != nil; item = item.Next {
		for _, hash := range item.Slice {
			delete(s.cache[hash].Events, store.eventCh)
		}
	}
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
	eventCh     chan<- struct{}
	parentStore *revisionDiffStore[TKey, TValue, THash]
	hashingFunc HashingFunc[TKey, THash]
	cache       map[THash]taskDiff[TKey, TValue]
	diffList    *list[THash]
	readList    *list[THash]
}

func newTaskDiffStore[TKey, TValue any, THash comparable](
	taskIndex uint64,
	eventCh chan<- struct{},
	parentStore *revisionDiffStore[TKey, TValue, THash],
	hashingFunc HashingFunc[TKey, THash],
) *taskDiffStore[TKey, TValue, THash] {
	return &taskDiffStore[TKey, TValue, THash]{
		taskIndex:   taskIndex,
		eventCh:     eventCh,
		parentStore: parentStore,
		hashingFunc: hashingFunc,
		cache:       map[THash]taskDiff[TKey, TValue]{},
		diffList:    newList[THash](),
		readList:    newList[THash](),
	}
}

func (s *taskDiffStore[TKey, TValue, THash]) Get(key TKey) (TValue, bool) {
	hash := s.hashingFunc(key)
	if diff, exists := s.cache[hash]; exists {
		return diff.Value, diff.Exists
	}

	value, exists := s.parentStore.Get(s.eventCh, key)
	s.readList.Append(hash)
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
