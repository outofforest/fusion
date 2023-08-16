package fusion

const degree = 32

type listItem[T any] struct {
	Slice []T
	Next  *listItem[T]
	next  *listItem[T]
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
		if l.tail.next == nil {
			l.tail.next = newListItem[T]()
		} else {
			l.tail.next.Slice = l.tail.next.Slice[:0]
			l.tail.next.Next = nil
		}
		l.tail.Next = l.tail.next
		l.tail = l.tail.next
	}
}

func (l *list[T]) Reset() {
	l.Head.Slice = l.Head.Slice[:0]
	l.Head.Next = nil
	l.tail = l.Head
}
