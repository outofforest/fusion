package fusion

const degree = 32

type listItem[T any] struct {
	Slice    []T
	Previous *listItem[T]
}

func newListItem[T any](previous *listItem[T]) *listItem[T] {
	return &listItem[T]{
		Slice:    make([]T, 0, degree),
		Previous: previous,
	}
}

type list[T any] struct {
	Tail *listItem[T]
}

func newList[T any]() *list[T] {
	return &list[T]{
		Tail: newListItem[T](nil),
	}
}

func (l *list[T]) Append(v T) {
	l.Tail.Slice = append(l.Tail.Slice, v)
	if len(l.Tail.Slice) == degree {
		l.Tail = newListItem[T](l.Tail)
	}
}
