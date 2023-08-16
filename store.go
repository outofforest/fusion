package fusion

import (
	"context"
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
	eventCh     chan any

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
		eventCh:     make(chan any, 100),
		cache:       map[THash]revisionDiff[TKey, TValue]{},
		diffList:    newList[THash](),
	}
}

type eventRead[THash comparable] struct {
	Hash THash
	Ch   chan<- struct{}
}

type eventMerged[THash comparable] struct {
	ReadList *list[THash]
	DiffList *list[THash]
	Ch       chan<- struct{}
	Error    error
	NextCh   chan<- struct{}
}

func (s *revisionDiffStore[TKey, TValue, THash]) RunEvents(ctx context.Context) error {
	events := map[THash]*list[chan<- struct{}]{}

	for event := range s.eventCh {
		switch e := event.(type) {
		case eventRead[THash]:
			chs := events[e.Hash]
			if chs == nil {
				chs = newList[chan<- struct{}]()
				events[e.Hash] = chs
			}
			chs.Append(e.Ch)
		case eventMerged[THash]:
			if e.Error == nil {
				sentEvents := map[chan<- struct{}]struct{}{}
				for item := e.DiffList.Head; item != nil; item = item.Next {
					for _, hash := range item.Slice {
						if l := events[hash]; l != nil {
							for chs := l.Head; chs != nil; chs = chs.Next {
								for _, ch := range chs.Slice {
									if _, exists := sentEvents[ch]; !exists {
										select {
										case ch <- struct{}{}:
										default:
										}

										sentEvents[ch] = struct{}{}
									}
								}
							}
							l.Reset()
						}
					}
				}
			}

			close(e.NextCh)
		}
	}

	return nil
}

func (s *revisionDiffStore[TKey, TValue, THash]) Get(eventCh chan<- struct{}, key TKey) (TValue, bool) {
	hash := s.hashingFunc(key)

	s.eventCh <- eventRead[THash]{
		Hash: hash,
		Ch:   eventCh,
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	if diff, exists := s.cache[hash]; exists {
		return diff.Value, diff.Exists
	}

	value, exists := s.parentStore.Get(key)
	return value, exists
}

func (s *revisionDiffStore[TKey, TValue, THash]) mergeTaskDiff(errHandler error, nextCh chan<- struct{}, store *taskDiffStore[TKey, TValue, THash]) {
	if errHandler == nil {
		s.mu.Lock()

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
			}
		}

		s.mu.Unlock()
	}

	s.eventCh <- eventMerged[THash]{
		ReadList: store.readList,
		DiffList: store.diffList,
		Ch:       store.eventCh,
		Error:    errHandler,
		NextCh:   nextCh,
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
