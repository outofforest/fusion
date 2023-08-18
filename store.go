package fusion

type diff[TKey, TValue any] struct {
	Key    TKey
	Value  TValue
	Exists bool
}

type revisionDiffStore[TKey, TValue any, THash comparable] struct {
	parentStore Store[TKey, TValue]
	hashingFunc HashingFunc[TKey, THash]
	cache       map[THash]diff[TKey, TValue]
	diffList    *list[THash]
}

func newRevisionDiffStore[TKey, TValue any, THash comparable](
	parentStore Store[TKey, TValue],
	hashingFunc HashingFunc[TKey, THash],
) *revisionDiffStore[TKey, TValue, THash] {
	return &revisionDiffStore[TKey, TValue, THash]{
		parentStore: parentStore,
		hashingFunc: hashingFunc,
		cache:       map[THash]diff[TKey, TValue]{},
		diffList:    newList[THash](),
	}
}

func (s *revisionDiffStore[TKey, TValue, THash]) Get(key TKey) (TValue, bool) {
	hash := s.hashingFunc(key)

	if diff, exists := s.cache[hash]; exists {
		return diff.Value, diff.Exists
	}

	value, exists := s.parentStore.Get(key)
	return value, exists
}

func (s *revisionDiffStore[TKey, TValue, THash]) mergeDiff(
	store *taskDiffStore[TKey, TValue, THash],
) {
	for item := store.diffList.Head; item != nil; item = item.Next {
		for _, hash := range item.Slice {
			if _, exists := s.cache[hash]; !exists {
				s.diffList.Append(hash)
			}
			s.cache[hash] = store.cache[hash]
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
	parentStore *revisionDiffStore[TKey, TValue, THash]
	hashingFunc HashingFunc[TKey, THash]
	diffList    *list[THash]
	cache       map[THash]diff[TKey, TValue]
}

func newTaskDiffStore[TKey, TValue any, THash comparable](
	parentStore *revisionDiffStore[TKey, TValue, THash],
	hashingFunc HashingFunc[TKey, THash],
	diffList *list[THash],
) *taskDiffStore[TKey, TValue, THash] {
	return &taskDiffStore[TKey, TValue, THash]{
		parentStore: parentStore,
		hashingFunc: hashingFunc,
		diffList:    diffList,
		cache:       map[THash]diff[TKey, TValue]{},
	}
}

func (s *taskDiffStore[TKey, TValue, THash]) Key(key TKey) KeyStore[TKey, TValue, THash] {
	return newKeyStore[TKey, TValue, THash](s, key, s.hashingFunc(key))
}

func (s *taskDiffStore[TKey, TValue, THash]) Reset() {
	for key := range s.cache {
		delete(s.cache, key)
	}
	s.diffList.Reset()
}

func (s *taskDiffStore[TKey, TValue, THash]) get(key TKey, hash THash) (TValue, bool) {
	if diff, exists := s.cache[hash]; exists {
		return diff.Value, diff.Exists
	}

	return s.parentStore.Get(key)
}

func (s *taskDiffStore[TKey, TValue, THash]) set(key TKey, hash THash, value TValue) {
	if _, exists := s.cache[hash]; !exists {
		s.diffList.Append(hash)
	}
	s.cache[hash] = diff[TKey, TValue]{
		Key:    key,
		Value:  value,
		Exists: true,
	}
}

func (s *taskDiffStore[TKey, TValue, THash]) delete(key TKey, hash THash) {
	if _, exists := s.cache[hash]; !exists {
		s.diffList.Append(hash)
	}
	s.cache[hash] = diff[TKey, TValue]{
		Key: key,
	}
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
