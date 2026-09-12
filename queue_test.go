package spms

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

// waitForParked blocks until n goroutines are parked on the queue, which lets the
// tests assert handoff behaviour without resorting to sleeps.
func waitForParked[T any](t *testing.T, q *Queue[T], n int) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for {
		q.buf.mu.Lock()
		waiters := q.buf.waiters
		q.buf.mu.Unlock()
		if waiters >= n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %d parked goroutines, have %d", n, waiters)
		}
		time.Sleep(50 * time.Microsecond)
	}
}

func TestQueue_DistributesEveryItemExactlyOnce(t *testing.T) {
	t.Parallel()

	const (
		itemCount = 500
		consumers = 8
	)

	q := New[int](WithCapacity(8))

	var (
		mu      sync.Mutex
		seen    = make(map[int]int, itemCount)
		workers sync.WaitGroup
	)
	for range consumers {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for {
				item, err := q.Next(context.Background())
				if err != nil {
					if !errors.Is(err, ErrClosed) {
						t.Errorf("Next: unexpected error %v", err)
					}
					return
				}
				mu.Lock()
				seen[item]++
				mu.Unlock()
			}
		}()
	}

	for item := range itemCount {
		if err := q.Publish(context.Background(), item); err != nil {
			t.Fatalf("Publish(%d): %v", item, err)
		}
	}
	q.Close()
	workers.Wait()

	if len(seen) != itemCount {
		t.Fatalf("received %d distinct items, want %d", len(seen), itemCount)
	}
	for item, count := range seen {
		if count != 1 {
			t.Fatalf("item %d was delivered %d times, want exactly once", item, count)
		}
	}
	if got := q.Stats().Delivered; got != itemCount {
		t.Fatalf("Delivered = %d, want %d", got, itemCount)
	}
}

func TestQueue_SingleConsumerSeesPublicationOrder(t *testing.T) {
	t.Parallel()

	const itemCount = 200

	q := New[int](WithCapacity(4))
	go func() {
		for item := range itemCount {
			if err := q.Publish(context.Background(), item); err != nil {
				t.Errorf("Publish(%d): %v", item, err)
				return
			}
		}
		q.Close()
	}()

	var got []int
	for item := range q.Items(context.Background()) {
		got = append(got, item)
	}

	want := make([]int, itemCount)
	for i := range want {
		want[i] = i
	}
	if !slices.Equal(got, want) {
		t.Fatalf("consumed %v, want %v", got, want)
	}
}

func TestQueue_PublishBlocksUntilSpaceIsAvailable(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	q := New[int](WithCapacity(1))
	if err := q.Publish(ctx, 1); err != nil {
		t.Fatalf("Publish(1): %v", err)
	}

	published := make(chan error, 1)
	go func() { published <- q.Publish(ctx, 2) }()
	waitForParked(t, q, 1)

	select {
	case err := <-published:
		t.Fatalf("Publish returned while the queue was still full: %v", err)
	default:
	}

	item, err := q.Next(ctx)
	if err != nil || item != 1 {
		t.Fatalf("Next() = (%v, %v), want (1, nil)", item, err)
	}

	select {
	case err := <-published:
		if err != nil {
			t.Fatalf("Publish after space was freed: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Publish did not resume after a consumer made room")
	}
}

func TestQueue_OverflowPolicies(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		policy        Overflow
		wantErr       error
		wantDrain     []int
		wantPublished uint64
		wantDropped   uint64
	}{
		{
			name:          "drop newest keeps the backlog and sheds the newcomer",
			policy:        OverflowDropNewest,
			wantDrain:     []int{1, 2},
			wantPublished: 2,
			wantDropped:   1,
		},
		{
			name:          "drop oldest evicts the front of the backlog",
			policy:        OverflowDropOldest,
			wantDrain:     []int{2, 3},
			wantPublished: 3,
			wantDropped:   1,
		},
		{
			name:          "error refuses the item and counts nothing",
			policy:        OverflowError,
			wantErr:       ErrFull,
			wantDrain:     []int{1, 2},
			wantPublished: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			q := New[int](WithCapacity(2), WithOverflow(tt.policy))

			var gotErr error
			for item := 1; item <= 3; item++ {
				if err := q.Publish(ctx, item); err != nil {
					gotErr = err
					break
				}
			}
			if !errors.Is(gotErr, tt.wantErr) {
				t.Fatalf("third Publish error = %v, want %v", gotErr, tt.wantErr)
			}
			if got := q.Drain(); !slices.Equal(got, tt.wantDrain) {
				t.Fatalf("Drain() = %v, want %v", got, tt.wantDrain)
			}

			stats := q.Stats()
			if stats.Published != tt.wantPublished {
				t.Errorf("Published = %d, want %d", stats.Published, tt.wantPublished)
			}
			if stats.Dropped != tt.wantDropped {
				t.Errorf("Dropped = %d, want %d", stats.Dropped, tt.wantDropped)
			}
		})
	}
}

func TestQueue_TryPublishNeverBlocks(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("buffered queue reports fullness", func(t *testing.T) {
		t.Parallel()

		q := New[int](WithCapacity(1))
		if err := q.TryPublish(1); err != nil {
			t.Fatalf("TryPublish(1): %v", err)
		}
		if err := q.TryPublish(2); !errors.Is(err, ErrFull) {
			t.Fatalf("TryPublish(2) = %v, want %v", err, ErrFull)
		}
		if got := q.Stats().Dropped; got != 0 {
			t.Fatalf("Dropped = %d, want 0: refusing an item is not dropping one", got)
		}
		if item, err := q.Next(ctx); err != nil || item != 1 {
			t.Fatalf("Next() = (%v, %v), want (1, nil)", item, err)
		}
	})

	t.Run("unbuffered queue needs a waiting consumer", func(t *testing.T) {
		t.Parallel()

		q := New[int]()
		if err := q.TryPublish(1); !errors.Is(err, ErrFull) {
			t.Fatalf("TryPublish with no consumer = %v, want %v", err, ErrFull)
		}

		received := make(chan int, 1)
		go func() {
			item, err := q.Next(ctx)
			if err == nil {
				received <- item
			}
		}()
		waitForParked(t, q, 1)

		if err := q.TryPublish(42); err != nil {
			t.Fatalf("TryPublish with a parked consumer: %v", err)
		}
		select {
		case item := <-received:
			if item != 42 {
				t.Fatalf("consumer received %d, want 42", item)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("parked consumer never received the item")
		}
	})
}

func TestQueue_RendezvousWaitsForAConsumer(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	q := New[string]()
	if got := q.Cap(); got != 0 {
		t.Fatalf("Cap() = %d, want 0", got)
	}

	published := make(chan error, 1)
	go func() { published <- q.Publish(ctx, "job") }()
	waitForParked(t, q, 1)

	// The offered item is visible while its handoff is pending, even though the
	// buffer has no capacity.
	if got := q.Len(); got != 1 {
		t.Fatalf("Len() = %d, want 1 while a handoff is pending", got)
	}
	select {
	case err := <-published:
		t.Fatalf("Publish returned before a consumer took the item: %v", err)
	default:
	}

	item, err := q.Next(ctx)
	if err != nil || item != "job" {
		t.Fatalf("Next() = (%q, %v), want (\"job\", nil)", item, err)
	}
	select {
	case err := <-published:
		if err != nil {
			t.Fatalf("Publish after the handoff: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Publish did not return after the handoff completed")
	}
}

func TestQueue_RendezvousSerialisesPendingPublishes(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	q := New[string]()

	first := make(chan error, 1)
	go func() { first <- q.Publish(ctx, "first") }()
	waitForParked(t, q, 1)

	// The single slot is occupied, so the second producer waits its turn rather
	// than overwriting the pending item.
	second := make(chan error, 1)
	go func() { second <- q.Publish(ctx, "second") }()
	waitForParked(t, q, 2)
	if got := q.Len(); got != 1 {
		t.Fatalf("Len() = %d, want 1 pending item", got)
	}

	item, err := q.Next(ctx)
	if err != nil || item != "first" {
		t.Fatalf("first Next() = (%q, %v), want (\"first\", nil)", item, err)
	}
	if publishErr := <-first; publishErr != nil {
		t.Fatalf("first Publish: %v", publishErr)
	}

	item, err = q.Next(ctx)
	if err != nil || item != "second" {
		t.Fatalf("second Next() = (%q, %v), want (\"second\", nil)", item, err)
	}
	if publishErr := <-second; publishErr != nil {
		t.Fatalf("second Publish: %v", publishErr)
	}
}

func TestQueue_RendezvousCloseReleasesWaitingPublishers(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	q := New[int]()

	first := make(chan error, 1)
	go func() { first <- q.Publish(ctx, 1) }()
	waitForParked(t, q, 1)

	second := make(chan error, 1)
	go func() { second <- q.Publish(ctx, 2) }()
	waitForParked(t, q, 2)

	q.Close()

	for i, published := range []chan error{first, second} {
		if err := <-published; !errors.Is(err, ErrClosed) {
			t.Fatalf("producer %d = %v, want %v", i, err, ErrClosed)
		}
	}
	if got := q.Stats().Dropped; got != 1 {
		t.Fatalf("Dropped = %d, want 1: only the offered item was discarded", got)
	}
	if err := q.Publish(ctx, 3); !errors.Is(err, ErrClosed) {
		t.Fatalf("Publish after Close = %v, want %v", err, ErrClosed)
	}
}

func TestQueue_RendezvousKeepsHandoffsDistinctUnderContention(t *testing.T) {
	t.Parallel()

	const (
		producers   = 4
		consumers   = 4
		perProducer = 100
	)

	ctx := context.Background()
	q := New[int]()

	var (
		mu       sync.Mutex
		received = make(map[int]int, producers*perProducer)
		workers  sync.WaitGroup
	)
	for range consumers {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for {
				item, err := q.Next(ctx)
				if err != nil {
					return
				}
				mu.Lock()
				received[item]++
				mu.Unlock()
			}
		}()
	}

	// Several producers contend for the single handoff slot, which is where a
	// publisher could mistake another publisher's item for its own.
	var publishers sync.WaitGroup
	for producer := range producers {
		publishers.Add(1)
		go func() {
			defer publishers.Done()
			for i := range perProducer {
				if err := q.Publish(ctx, producer*perProducer+i); err != nil {
					t.Errorf("Publish: %v", err)
					return
				}
			}
		}()
	}
	publishers.Wait()
	q.Close()
	workers.Wait()

	if len(received) != producers*perProducer {
		t.Fatalf("received %d distinct items, want %d", len(received), producers*perProducer)
	}
	for item, count := range received {
		if count != 1 {
			t.Fatalf("item %d delivered %d times, want exactly once", item, count)
		}
	}
}

func TestQueue_RendezvousHonoursContext(t *testing.T) {
	t.Parallel()

	t.Run("publisher waiting for the slot", func(t *testing.T) {
		t.Parallel()

		q := New[int]()

		blocking := make(chan error, 1)
		go func() { blocking <- q.Publish(context.Background(), 1) }()
		waitForParked(t, q, 1)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := q.Publish(ctx, 2); !errors.Is(err, context.Canceled) {
			t.Fatalf("Publish while the slot is busy = %v, want %v", err, context.Canceled)
		}

		// The first publisher is untouched and still completes its handoff.
		if item, err := q.Next(context.Background()); err != nil || item != 1 {
			t.Fatalf("Next() = (%d, %v), want (1, nil)", item, err)
		}
		if err := <-blocking; err != nil {
			t.Fatalf("first Publish: %v", err)
		}
	})

	t.Run("publisher waiting for its own handoff", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(context.Background())
		q := New[int]()

		published := make(chan error, 1)
		go func() { published <- q.Publish(ctx, 7) }()
		waitForParked(t, q, 1)

		cancel()
		if err := <-published; !errors.Is(err, context.Canceled) {
			t.Fatalf("Publish = %v, want %v", err, context.Canceled)
		}

		// The abandoned item is still visible, so a consumer can still take it.
		if item, err := q.Next(context.Background()); err != nil || item != 7 {
			t.Fatalf("Next() = (%d, %v), want (7, nil)", item, err)
		}
	})
}

func TestQueue_CloseDrainsBacklogThenReportsClosed(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	q := New[int](WithCapacity(4))
	for item := 1; item <= 3; item++ {
		if err := q.Publish(ctx, item); err != nil {
			t.Fatalf("Publish(%d): %v", item, err)
		}
	}

	q.Close()
	q.Close() // idempotent

	if !q.Closed() {
		t.Fatal("Closed() = false after Close()")
	}
	if err := q.Publish(ctx, 4); !errors.Is(err, ErrClosed) {
		t.Fatalf("Publish after Close = %v, want %v", err, ErrClosed)
	}

	for want := 1; want <= 3; want++ {
		item, err := q.Next(ctx)
		if err != nil {
			t.Fatalf("Next() after Close = %v, want buffered item %d", err, want)
		}
		if item != want {
			t.Fatalf("Next() = %d, want %d", item, want)
		}
	}
	if _, err := q.Next(ctx); !errors.Is(err, ErrClosed) {
		t.Fatalf("Next() on a drained queue = %v, want %v", err, ErrClosed)
	}
}

func TestQueue_CloseDiscardsAPendingRendezvousHandoff(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	q := New[int]()

	published := make(chan error, 1)
	go func() { published <- q.Publish(ctx, 7) }()
	waitForParked(t, q, 1)

	q.Close()
	if err := <-published; !errors.Is(err, ErrClosed) {
		t.Fatalf("Publish interrupted by Close = %v, want %v", err, ErrClosed)
	}
	if _, err := q.Next(ctx); !errors.Is(err, ErrClosed) {
		t.Fatalf("Next() = %v, want %v", err, ErrClosed)
	}
	if got := q.Stats().Dropped; got != 1 {
		t.Fatalf("Dropped = %d, want 1: the discarded handoff must be visible", got)
	}
}

func TestQueue_HonoursContextCancellation(t *testing.T) {
	t.Parallel()

	t.Run("Next on an empty queue", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		q := New[int](WithCapacity(1))
		if _, err := q.Next(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("Next() = %v, want %v", err, context.Canceled)
		}
	})

	t.Run("Next with an expired deadline", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		defer cancel()
		<-ctx.Done()

		q := New[int](WithCapacity(1))
		if _, err := q.Next(ctx); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Next() = %v, want %v", err, context.DeadlineExceeded)
		}
	})

	t.Run("Publish on a full queue", func(t *testing.T) {
		t.Parallel()

		q := New[int](WithCapacity(1))
		if err := q.Publish(context.Background(), 1); err != nil {
			t.Fatalf("Publish(1): %v", err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := q.Publish(ctx, 2); !errors.Is(err, context.Canceled) {
			t.Fatalf("Publish(2) = %v, want %v", err, context.Canceled)
		}
	})
}

func TestQueue_DrainReturnsBacklogInOrder(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	q := New[int](WithCapacity(8))
	for item := 1; item <= 5; item++ {
		if err := q.Publish(ctx, item); err != nil {
			t.Fatalf("Publish(%d): %v", item, err)
		}
	}

	if got := q.Drain(); !slices.Equal(got, []int{1, 2, 3, 4, 5}) {
		t.Fatalf("Drain() = %v, want [1 2 3 4 5]", got)
	}
	if got := q.Drain(); got != nil {
		t.Fatalf("Drain() on an empty queue = %v, want nil", got)
	}
	if got := q.Stats().Delivered; got != 5 {
		t.Fatalf("Delivered = %d, want 5", got)
	}
}

func TestQueue_ItemsStopsOnBreakAndKeepsTheRest(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	q := New[int](WithCapacity(8))
	for item := 1; item <= 5; item++ {
		if err := q.Publish(ctx, item); err != nil {
			t.Fatalf("Publish(%d): %v", item, err)
		}
	}

	var got []int
	for item := range q.Items(ctx) {
		got = append(got, item)
		if len(got) == 2 {
			break
		}
	}
	if !slices.Equal(got, []int{1, 2}) {
		t.Fatalf("Items() yielded %v, want [1 2]", got)
	}
	if got := q.Len(); got != 3 {
		t.Fatalf("Len() = %d, want 3 remaining items", got)
	}
}

func TestQueue_SupportsMultipleProducersSafely(t *testing.T) {
	t.Parallel()

	const (
		producers     = 4
		perProducer   = 250
		consumerCount = 4
	)

	ctx := context.Background()
	q := New[int](WithCapacity(16))

	var (
		mu       sync.Mutex
		received = make(map[int]int, producers*perProducer)
		workers  sync.WaitGroup
	)
	for range consumerCount {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for {
				item, err := q.Next(ctx)
				if err != nil {
					if !errors.Is(err, ErrClosed) {
						t.Errorf("Next: unexpected error %v", err)
					}
					return
				}
				mu.Lock()
				received[item]++
				mu.Unlock()
			}
		}()
	}

	var producersDone sync.WaitGroup
	for producer := range producers {
		producersDone.Add(1)
		go func() {
			defer producersDone.Done()
			for i := range perProducer {
				if err := q.Publish(ctx, producer*perProducer+i); err != nil {
					t.Errorf("Publish: %v", err)
					return
				}
			}
		}()
	}
	producersDone.Wait()
	q.Close()
	workers.Wait()

	if len(received) != producers*perProducer {
		t.Fatalf("received %d distinct items, want %d", len(received), producers*perProducer)
	}
	for item, count := range received {
		if count != 1 {
			t.Fatalf("item %d delivered %d times, want exactly once", item, count)
		}
	}
}

func TestQueue_RejectsInvalidOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		call func()
		want string
	}{
		{
			name: "negative capacity",
			call: func() { New[int](WithCapacity(-1)) },
			want: "negative capacity -1",
		},
		{
			name: "unknown overflow policy",
			call: func() { New[int](WithOverflow(Overflow(42))) },
			want: "invalid overflow policy 42",
		},
		{
			name: "unknown overflow policy on a subscription",
			call: func() { NewBroadcaster[int]().Subscribe(WithOverflow(Overflow(-3))) },
			want: "invalid overflow policy -3",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			defer func() {
				recovered := recover()
				if recovered == nil {
					t.Fatalf("expected a panic mentioning %q", tt.want)
				}
				if got := fmt.Sprint(recovered); !strings.Contains(got, tt.want) {
					t.Fatalf("panic = %q, want it to mention %q", got, tt.want)
				}
			}()
			tt.call()
		})
	}
}

func TestQueue_IgnoresNilOptions(t *testing.T) {
	t.Parallel()

	// A nil option is what conditional configuration produces, so it must be
	// skipped while the options that follow it still apply.
	var unset Option
	q := New[int](nil, unset, WithCapacity(2))
	if got := q.Cap(); got != 2 {
		t.Fatalf("Cap() = %d, want 2: options after a nil one must still apply", got)
	}
}

func TestQueue_LeavesNoGoroutinesBehind(t *testing.T) {
	// This test is deliberately not parallel: it compares the goroutine count
	// before and after the library is exercised, and concurrent tests would make
	// that comparison meaningless.
	base := runtime.NumGoroutine()

	ctx := context.Background()
	for range 20 {
		q := New[int](WithCapacity(4))

		var workers sync.WaitGroup
		for range 2 {
			workers.Add(1)
			go func() {
				defer workers.Done()
				for {
					if _, err := q.Next(ctx); err != nil {
						return
					}
				}
			}()
		}

		for item := range 64 {
			if err := q.Publish(ctx, item); err != nil {
				t.Fatalf("Publish(%d): %v", item, err)
			}
		}
		q.Close()
		workers.Wait()
	}

	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > base && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := runtime.NumGoroutine(); got > base {
		t.Fatalf("goroutines grew from %d to %d: the library must not start any", base, got)
	}
}
