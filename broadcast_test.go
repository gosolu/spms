package spmc

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"
)

func TestBroadcaster_FansOutToEverySubscription(t *testing.T) {
	t.Parallel()

	const (
		subscribers = 3
		itemCount   = 100
	)

	ctx := context.Background()
	b := NewBroadcaster[int](WithCapacity(4))

	subs := make([]*Subscription[int], 0, subscribers)
	for range subscribers {
		subs = append(subs, b.Subscribe())
	}
	if got := b.Subscribers(); got != subscribers {
		t.Fatalf("Subscribers() = %d, want %d", got, subscribers)
	}

	// Each subscription is drained by its own goroutine: with the default blocking
	// policy, publishing into a subscription nobody reads is meant to stall.
	received := make([][]int, subscribers)
	var workers sync.WaitGroup
	for i, sub := range subs {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for item := range sub.Items(ctx) {
				received[i] = append(received[i], item)
			}
		}()
	}

	for item := range itemCount {
		if err := b.Publish(ctx, item); err != nil {
			t.Fatalf("Publish(%d): %v", item, err)
		}
	}
	b.Close()
	workers.Wait()

	want := make([]int, itemCount)
	for i := range want {
		want[i] = i
	}
	for i, got := range received {
		if !slices.Equal(got, want) {
			t.Fatalf("subscription %d received %v, want %v", i, got, want)
		}
	}
}

func TestBroadcaster_SubscriptionOnlySeesItemsPublishedAfterItJoined(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := NewBroadcaster[int](WithCapacity(4))

	if err := b.Publish(ctx, 1); err != nil {
		t.Fatalf("Publish with no subscribers: %v", err)
	}
	sub := b.Subscribe()
	if err := b.Publish(ctx, 2); err != nil {
		t.Fatalf("Publish(2): %v", err)
	}
	b.Close()

	var got []int
	for item := range sub.Items(ctx) {
		got = append(got, item)
	}
	if !slices.Equal(got, []int{2}) {
		t.Fatalf("late subscription received %v, want [2]", got)
	}
	if published := b.Stats().Published; published != 2 {
		t.Fatalf("Published = %d, want 2", published)
	}
}

func TestBroadcaster_CloseReleasesABlockedPublisher(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := NewBroadcaster[int](WithCapacity(1))
	sub := b.Subscribe()
	if err := b.Publish(ctx, 1); err != nil {
		t.Fatalf("Publish(1): %v", err)
	}

	published := make(chan error, 1)
	go func() { published <- b.Publish(ctx, 2) }()
	waitForSubscriptionParked(t, sub, 1)

	b.Close()
	if err := <-published; !errors.Is(err, ErrClosed) {
		t.Fatalf("Publish interrupted by Close = %v, want %v", err, ErrClosed)
	}
	if got := sub.Drain(); !slices.Equal(got, []int{1}) {
		t.Fatalf("subscription backlog = %v, want [1]", got)
	}
}

// waitForSubscriptionParked blocks until n goroutines are parked on the
// subscription's buffer.
func waitForSubscriptionParked[T any](t *testing.T, sub *Subscription[T], n int) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for {
		sub.buf.mu.Lock()
		waiters := sub.buf.waiters
		sub.buf.mu.Unlock()
		if waiters >= n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %d parked goroutines, have %d", n, waiters)
		}
		time.Sleep(50 * time.Microsecond)
	}
}

func TestBroadcaster_CloseDrainsEverySubscription(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := NewBroadcaster[int](WithCapacity(8))
	first := b.Subscribe()
	second := b.Subscribe()

	for item := 1; item <= 3; item++ {
		if err := b.Publish(ctx, item); err != nil {
			t.Fatalf("Publish(%d): %v", item, err)
		}
	}

	b.Close()
	b.Close() // idempotent

	if !b.Closed() {
		t.Fatal("Closed() = false after Close()")
	}
	if err := b.Publish(ctx, 4); !errors.Is(err, ErrClosed) {
		t.Fatalf("Publish after Close = %v, want %v", err, ErrClosed)
	}
	for _, sub := range []*Subscription[int]{first, second} {
		var got []int
		for item := range sub.Items(ctx) {
			got = append(got, item)
		}
		if !slices.Equal(got, []int{1, 2, 3}) {
			t.Fatalf("subscription drained %v, want [1 2 3]", got)
		}
	}
}

func TestBroadcaster_SubscribeAfterCloseReturnsAClosedSubscription(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := NewBroadcaster[int](WithCapacity(4))
	b.Close()

	sub := b.Subscribe()
	if _, err := sub.Next(ctx); !errors.Is(err, ErrClosed) {
		t.Fatalf("Next() = %v, want %v", err, ErrClosed)
	}
	sub.Close() // must be safe even though it was never registered
	if got := b.Subscribers(); got != 0 {
		t.Fatalf("Subscribers() = %d, want 0", got)
	}
}

func TestBroadcaster_UnsubscribeLeavesOtherConsumersUntouched(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := NewBroadcaster[int](WithCapacity(4))
	leaving := b.Subscribe()
	staying := b.Subscribe()

	if err := b.Publish(ctx, 1); err != nil {
		t.Fatalf("Publish(1): %v", err)
	}
	leaving.Close()
	if got := b.Subscribers(); got != 1 {
		t.Fatalf("Subscribers() = %d, want 1", got)
	}
	if err := b.Publish(ctx, 2); err != nil {
		t.Fatalf("Publish(2): %v", err)
	}
	b.Close()

	// A subscription that left keeps whatever it had already received, so its
	// consumer can drain the backlog before it observes ErrClosed.
	var left []int
	for item := range leaving.Items(ctx) {
		left = append(left, item)
	}
	if !slices.Equal(left, []int{1}) {
		t.Fatalf("closed subscription drained %v, want [1]", left)
	}

	var kept []int
	for item := range staying.Items(ctx) {
		kept = append(kept, item)
	}
	if !slices.Equal(kept, []int{1, 2}) {
		t.Fatalf("remaining subscription drained %v, want [1 2]", kept)
	}
}

func TestBroadcaster_PublishSkipsASubscriptionThatClosedMidFlight(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := NewBroadcaster[int](WithCapacity(4))
	leaving := b.Subscribe()
	staying := b.Subscribe()

	// Reproduce the window inside Subscription.Close between closing the buffer and
	// unregistering it: a publisher holding an older snapshot must skip that
	// subscription instead of failing the whole fan-out.
	leaving.buf.close()

	if err := b.Publish(ctx, 1); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if got := staying.Drain(); !slices.Equal(got, []int{1}) {
		t.Fatalf("remaining subscription received %v, want [1]", got)
	}
	if got := b.Stats().Published; got != 1 {
		t.Fatalf("Published = %d, want 1", got)
	}
}

func TestBroadcaster_LossySubscriptionDoesNotStallTheProducer(t *testing.T) {
	t.Parallel()

	const itemCount = 100

	ctx := context.Background()
	b := NewBroadcaster[int](WithCapacity(128))
	lossy := b.Subscribe(WithCapacity(2), WithOverflow(OverflowDropNewest))
	exact := b.Subscribe()

	for item := range itemCount {
		if err := b.Publish(ctx, item); err != nil {
			t.Fatalf("Publish(%d): %v", item, err)
		}
	}
	b.Close()

	if got := lossy.Stats().Dropped; got != itemCount-2 {
		t.Fatalf("lossy subscription dropped %d items, want %d", got, itemCount-2)
	}
	if got := lossy.Drain(); !slices.Equal(got, []int{0, 1}) {
		t.Fatalf("lossy subscription kept %v, want [0 1]", got)
	}

	var got []int
	for item := range exact.Items(ctx) {
		got = append(got, item)
	}
	if len(got) != itemCount {
		t.Fatalf("exact subscription received %d items, want %d", len(got), itemCount)
	}
}

func TestBroadcaster_PublishHonoursContextWhileWaitingForASubscriber(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := NewBroadcaster[int](WithCapacity(1))
	sub := b.Subscribe()

	if err := b.Publish(ctx, 1); err != nil {
		t.Fatalf("Publish(1): %v", err)
	}

	waitCtx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	if err := b.Publish(waitCtx, 2); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Publish(2) = %v, want %v", err, context.DeadlineExceeded)
	}

	// The interrupted publish must not have corrupted the subscription backlog.
	if got := sub.Drain(); !slices.Equal(got, []int{1}) {
		t.Fatalf("subscription backlog = %v, want [1]", got)
	}
}

func TestBroadcaster_SubscriptionCloseIsSafeDuringPublish(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := NewBroadcaster[int](WithCapacity(64))

	var (
		publishers sync.WaitGroup
		churn      sync.WaitGroup
	)
	for range 4 {
		publishers.Add(1)
		go func() {
			defer publishers.Done()
			for item := range 200 {
				if err := b.Publish(ctx, item); err != nil && !errors.Is(err, ErrClosed) {
					t.Errorf("Publish: %v", err)
					return
				}
			}
		}()
	}
	for range 4 {
		churn.Add(1)
		go func() {
			defer churn.Done()
			for range 50 {
				sub := b.Subscribe()
				// A subscription created after the last publish has nothing to
				// receive, so the deadline is what keeps this churn bounded.
				recvCtx, cancel := context.WithTimeout(ctx, time.Millisecond)
				_, err := sub.Next(recvCtx)
				cancel()
				if err != nil && !errors.Is(err, ErrClosed) && !errors.Is(err, context.DeadlineExceeded) {
					t.Errorf("Next: %v", err)
				}
				sub.Close()
			}
		}()
	}

	churn.Wait()
	publishers.Wait()
	b.Close()

	if got := b.Subscribers(); got != 0 {
		t.Fatalf("Subscribers() = %d, want 0 after every subscription closed", got)
	}
}

func TestBroadcaster_StatsAggregateSubscriptions(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b := NewBroadcaster[int](WithCapacity(8))
	sub := b.Subscribe()

	for item := 1; item <= 3; item++ {
		if err := b.Publish(ctx, item); err != nil {
			t.Fatalf("Publish(%d): %v", item, err)
		}
	}
	if _, err := sub.Next(ctx); err != nil {
		t.Fatalf("Next: %v", err)
	}

	if got := sub.Cap(); got != 8 {
		t.Errorf("Cap() = %d, want 8", got)
	}
	if got := sub.Len(); got != 2 {
		t.Errorf("Len() = %d, want 2", got)
	}
	stats := b.Stats()
	if stats.Published != 3 {
		t.Errorf("Published = %d, want 3", stats.Published)
	}
	if stats.Delivered != 1 {
		t.Errorf("Delivered = %d, want 1", stats.Delivered)
	}
	if stats.Buffered != 2 {
		t.Errorf("Buffered = %d, want 2", stats.Buffered)
	}
}
