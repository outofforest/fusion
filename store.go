package fusion

import (
	"sync"
)

type revisionDiff[TKey, TValue any] struct {
	Key    TKey
	Value  TValue
	Exists bool
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

	mu        sync.RWMutex
	taskIndex uint64
	events    map[THash]map[chan<- struct{}]uint64
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
		events:      map[THash]map[chan<- struct{}]uint64{},
		cache:       map[THash]revisionDiff[TKey, TValue]{},
		diffList:    newList[THash](),
	}
}

func (s *revisionDiffStore[TKey, TValue, THash]) Get(eventCh chan<- struct{}, key TKey) (TValue, bool) {
	hash := s.hashingFunc(key)

	s.mu.Lock()

	if _, exists := s.events[hash][eventCh]; !exists {
		if s.events[hash] == nil {
			s.events[hash] = map[chan<- struct{}]uint64{}
		}
		s.events[hash][eventCh] = s.taskIndex
	}

	if diff, exists := s.cache[hash]; exists {
		s.mu.Unlock()
		return diff.Value, diff.Exists
	}

	s.mu.Unlock()

	value, exists := s.parentStore.Get(key)
	return value, exists
}

func (s *revisionDiffStore[TKey, TValue, THash]) mergeTaskDiff(store *taskDiffStore[TKey, TValue, THash]) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.taskIndex = store.taskIndex

	for item := store.readList.Head; item != nil; item = item.Next {
		for _, hash := range item.Slice {
			delete(s.events[hash], store.eventCh)
			if len(s.events[hash]) == 0 {
				delete(s.events, hash)
			}
		}
	}

	sentEvents := map[chan<- struct{}]struct{}{}
	for item := store.diffList.Head; item != nil; item = item.Next {
		for _, hash := range item.Slice {
			if _, exists := s.cache[hash]; !exists {
				s.diffList.Append(hash)
			}
			diff := store.cache[hash]
			s.cache[hash] = revisionDiff[TKey, TValue]{
				Key:    diff.Key,
				Value:  diff.Value,
				Exists: diff.Exists,
			}

			for eventCh, taskIndex := range s.events[hash] {
				if store.taskIndex > taskIndex {
					if _, exists := sentEvents[eventCh]; !exists {
						select {
						case eventCh <- struct{}{}:
						default:
						}

						sentEvents[eventCh] = struct{}{}
					}
					delete(s.events[hash], eventCh)
				}
			}
			if len(s.events[hash]) == 0 {
				delete(s.events, hash)
			}
		}
	}
}

func (s *revisionDiffStore[TKey, TValue, THash]) cleanEvents(store *taskDiffStore[TKey, TValue, THash]) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for item := store.readList.Head; item != nil; item = item.Next {
		for _, hash := range item.Slice {
			delete(s.events[hash], store.eventCh)
			if len(s.events[hash]) == 0 {
				delete(s.events, hash)
			}
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
