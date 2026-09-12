package spms

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"
)

func TestBuffer_RingWrapsAndKeepsFIFOOrder(t *testing.T) {
	t.Parallel()

	b := newBuffer[int](config{capacity: 2, overflow: OverflowBlock})
	ctx := context.Background()

	for item := 1; item <= 10; item++ {
		if err := b.push(ctx, item, OverflowBlock); err != nil {
			t.Fatalf("push(%d): %v", item, err)
		}
		got, err := b.pop(ctx)
		if err != nil {
			t.Fatalf("pop(): %v", err)
		}
		if got != item {
			t.Fatalf("pop() = %d, want %d", got, item)
		}
	}
	if got := b.length(); got != 0 {
		t.Fatalf("length() = %d, want 0", got)
	}
}

func TestBuffer_ReleasesHandedOverPayloads(t *testing.T) {
	t.Parallel()

	type payload struct{ id int }

	b := newBuffer[*payload](config{capacity: 2, overflow: OverflowBlock})
	ctx := context.Background()

	if err := b.push(ctx, &payload{id: 1}, OverflowBlock); err != nil {
		t.Fatalf("push: %v", err)
	}
	if err := b.push(ctx, &payload{id: 2}, OverflowBlock); err != nil {
		t.Fatalf("push: %v", err)
	}
	if _, err := b.pop(ctx); err != nil {
		t.Fatalf("pop: %v", err)
	}

	// A queue that keeps pointing at consumed payloads would pin them for the
	// lifetime of the process, so the vacated slot must be cleared.
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.ring[0] != nil {
		t.Fatalf("ring slot %d still references %+v after pop", 0, b.ring[0])
	}
}

func TestBuffer_CloseDiscardsAPendingHandoff(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := newBuffer[int](config{overflow: OverflowBlock})

	published := make(chan error, 1)
	go func() { published <- b.push(ctx, 7, OverflowBlock) }()

	deadline := time.Now().Add(2 * time.Second)
	for {
		b.mu.Lock()
		pending := b.hasHandoff
		b.mu.Unlock()
		if pending {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("push never offered an item")
		}
		time.Sleep(50 * time.Microsecond)
	}

	b.close()
	if err := <-published; !errors.Is(err, ErrClosed) {
		t.Fatalf("push after close = %v, want %v", err, ErrClosed)
	}
	if got := b.length(); got != 0 {
		t.Fatalf("length() = %d, want 0", got)
	}
	if got := b.stats().Dropped; got != 1 {
		t.Fatalf("Dropped = %d, want 1", got)
	}
	if _, err := b.pop(ctx); !errors.Is(err, ErrClosed) {
		t.Fatalf("pop() = %v, want %v", err, ErrClosed)
	}
}

func TestBuffer_NonBlockingPushOnRendezvousNeedsAConsumer(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := newBuffer[int](config{overflow: OverflowBlock})

	if err := b.push(ctx, 1, OverflowError); !errors.Is(err, ErrFull) {
		t.Fatalf("non-blocking push with no consumer = %v, want %v", err, ErrFull)
	}
	if err := b.push(ctx, 1, OverflowDropNewest); err != nil {
		t.Fatalf("dropping push with no consumer = %v, want nil", err)
	}
	if got := b.stats(); got.Dropped != 1 || got.Published != 0 {
		t.Fatalf("stats = %+v, want one drop and no published item", got)
	}
}

func TestBuffer_DropOldestEvictsTheFront(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := newBuffer[int](config{capacity: 2, overflow: OverflowDropOldest})

	for item := 1; item <= 3; item++ {
		if err := b.push(ctx, item, OverflowDropOldest); err != nil {
			t.Fatalf("push(%d): %v", item, err)
		}
	}

	if got := b.drain(); !slices.Equal(got, []int{2, 3}) {
		t.Fatalf("drain() = %v, want [2 3]", got)
	}
	stats := b.stats()
	if stats.Published != 3 || stats.Dropped != 1 || stats.Delivered != 2 {
		t.Fatalf("stats = %+v, want 3 published, 1 dropped, 2 delivered", stats)
	}
}
