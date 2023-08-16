package fusion

const degree = 32

type listItem[T any] struct {
	Slice []T
	Next  *listItem[T]
}

func newListItem[T any]() *listItem[T] {
	return &listItem[T]{
		Slice: make([]T, 0, degree),
	}
}

type list[T any] struct {
	Head *listItem[T]
	tail *listItem[T]
}

func newList[T any]() *list[T] {
	item := newListItem[T]()
	return &list[T]{
		Head: item,
		tail: item,
	}
}

func (l *list[T]) Append(v T) {
	l.tail.Slice = append(l.tail.Slice, v)
	if len(l.tail.Slice) == degree {
		if l.tail.Next == nil {
			l.tail.Next = newListItem[T]()
		} else {
			l.tail.Next.Slice = l.tail.Next.Slice[:1]
		}
		l.tail = l.tail.Next
	}
}

func (l *list[T]) Reset() {
	l.Head.Slice = l.Head.Slice[:1]
	l.tail = l.Head
}
