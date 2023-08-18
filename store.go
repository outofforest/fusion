package fusion

type revisionDiff[TKey, TValue any] struct {
	Key    TKey
	Value  TValue
	Exists bool
}

type taskDiff[TKey, TValue any] struct {
	Key    TKey
	Value  TValue
	Exists bool
}

type revisionDiffStore[TKey, TValue any, THash comparable] struct {
	parentStore Store[TKey, TValue]
	hashingFunc HashingFunc[TKey, THash]
	cache       map[THash]revisionDiff[TKey, TValue]
	diffList    *list[THash]
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

func (s *revisionDiffStore[TKey, TValue, THash]) Get(key TKey) (TValue, bool) {
	hash := s.hashingFunc(key)

	if diff, exists := s.cache[hash]; exists {
		return diff.Value, diff.Exists
	}

	value, exists := s.parentStore.Get(key)
	return value, exists
}

func (s *revisionDiffStore[TKey, TValue, THash]) mergeDiff(
	diffList *list[THash],
	cache map[THash]revisionDiff[TKey, TValue],
) {
	for item := diffList.Head; item != nil; item = item.Next {
		for _, hash := range item.Slice {
			if _, exists := s.cache[hash]; !exists {
				s.diffList.Append(hash)
			}
			s.cache[hash] = cache[hash]
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
	hashingFunc HashingFunc[TKey, THash]
	cache       map[THash]taskDiff[TKey, TValue]

	parentStore *revisionDiffStore[TKey, TValue, THash]
	keyList     *list[THash]
	diffList    *list[THash]

	parentStoreSealed *revisionDiffStore[TKey, TValue, THash]
	keyListSealed     *list[THash]
	diffListSealed    *list[THash]
	sealCallback      func(keyList *list[THash])
}

func newTaskDiffStore[TKey, TValue any, THash comparable](
	parentStore *revisionDiffStore[TKey, TValue, THash],
	hashingFunc HashingFunc[TKey, THash],
	keyList *list[THash],
	diffList *list[THash],
) *taskDiffStore[TKey, TValue, THash] {
	return &taskDiffStore[TKey, TValue, THash]{
		parentStore: parentStore,
		hashingFunc: hashingFunc,
		keyList:     keyList,
		diffList:    diffList,
	}
}

func (s *taskDiffStore[TKey, TValue, THash]) Key(key TKey) KeyStore[TKey, TValue, THash] {
	hash := s.hashingFunc(key)
	s.keyList.Append(hash)
	return newKeyStore[TKey, TValue, THash](s, key, hash)
}

func (s *taskDiffStore[TKey, TValue, THash]) Seal() {
	s.cache = map[THash]taskDiff[TKey, TValue]{}
	s.parentStoreSealed = s.parentStore
	s.parentStore = nil
	s.keyListSealed = s.keyList
	s.keyList = nil
	s.diffListSealed = s.diffList
	s.diffList = nil

	sealCallback := s.sealCallback
	s.sealCallback = nil
	sealCallback(s.keyListSealed)
}

func (s *taskDiffStore[TKey, TValue, THash]) Reset(sealCallback func(keyList *list[THash])) {
	s.cache = nil
	if s.parentStore == nil {
		s.parentStore = s.parentStoreSealed
	}
	if s.keyList == nil {
		s.keyList = s.keyListSealed
	}
	s.keyList.Reset()
	if s.diffList == nil {
		s.diffList = s.diffListSealed
	}
	s.diffList.Reset()
	s.sealCallback = sealCallback
}

func (s *taskDiffStore[TKey, TValue, THash]) get(key TKey, hash THash) (TValue, bool) {
	if diff, exists := s.cache[hash]; exists {
		return diff.Value, diff.Exists
	}

	return s.parentStoreSealed.Get(key)
}

func (s *taskDiffStore[TKey, TValue, THash]) set(key TKey, hash THash, value TValue) {
	if _, exists := s.cache[hash]; !exists {
		s.diffListSealed.Append(hash)
	}
	s.cache[hash] = taskDiff[TKey, TValue]{
		Key:    key,
		Value:  value,
		Exists: true,
	}
}

func (s *taskDiffStore[TKey, TValue, THash]) delete(key TKey, hash THash) {
	if _, exists := s.cache[hash]; !exists {
		s.diffListSealed.Append(hash)
	}
	s.cache[hash] = taskDiff[TKey, TValue]{
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
