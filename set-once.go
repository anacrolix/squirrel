package squirrel

// This is useful for stmt result function callbacks, which we often expect to run a specific number
// of times.
type setOnce[T any] struct {
	value T
	ok    bool
}

func (me *setOnce[T]) Set(t T) {
	if me.ok {
		panic("set more than once")
	}
	me.value = t
	me.ok = true
}

func (me *setOnce[T]) Ok() bool {
	return me.ok
}

func (me *setOnce[T]) Value() T {
	if !me.ok {
		panic("value not set")
	}
	return me.value
}
