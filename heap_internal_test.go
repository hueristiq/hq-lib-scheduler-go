package scheduler

import (
	"container/heap"
	"testing"
	"time"
)

func TestEntryHeapOrdering(t *testing.T) {
	t.Parallel()

	base := time.Unix(0, 0)

	entries := []*entry{
		{id: 2, next: base.Add(2 * time.Second)},
		{id: 1, next: base.Add(2 * time.Second)},
		{id: 3, next: base.Add(1 * time.Second)},
		{id: 4, next: base},
	}

	h := entryHeap(entries)

	heap.Init(&h)

	var order []int

	for h.Len() > 0 {
		e := heap.Pop(&h).(*entry)

		order = append(order, e.id)
	}

	want := []int{4, 3, 1, 2}

	if len(order) != len(want) {
		t.Fatalf("order = %v, want %v", order, want)
	}

	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("order = %v, want %v", order, want)
		}
	}
}

func TestEntryHeapIndexes(t *testing.T) {
	t.Parallel()

	first := &entry{id: 1, next: time.Unix(0, 0)}
	second := &entry{id: 2, next: time.Unix(1, 0)}

	h := entryHeap{}
	h.Push(first)
	h.Push(second)

	if first.heapIndex != 0 || second.heapIndex != 1 {
		t.Fatalf("initial indexes = %d, %d; want 0, 1", first.heapIndex, second.heapIndex)
	}

	h.Swap(0, 1)

	if first.heapIndex != 1 || second.heapIndex != 0 {
		t.Fatalf("swapped indexes = %d, %d; want 1, 0", first.heapIndex, second.heapIndex)
	}

	if h.Less(0, 1) {
		t.Fatal("Less(0, 1) = true, want false after swap")
	}

	h.Push(&entry{id: 3, next: time.Unix(2, 0)})

	if got := h.Len(); got != 3 {
		t.Fatalf("Len() = %d, want 3", got)
	}

	popped := h.Pop().(*entry)

	if popped.heapIndex != -1 {
		t.Fatalf("popped heapIndex = %d, want -1", popped.heapIndex)
	}
}
