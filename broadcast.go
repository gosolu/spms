package spmc

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"sync"
	"sync/atomic"
)

// Broadcaster is the fan-out half of the SPMC toolbox: a single producer publishes
// an item and every registered [Subscription] receives its own copy.
//
// Subscriptions come and go at any time. Each one owns a bounded buffer, so a
// consumer that stops keeping up cannot corrupt the others; what it does instead is
// decided by that subscription's [Overflow] policy. Under the default
// [OverflowBlock] the slowest subscription throttles the producer, which is the
// right default when every consumer must see every item, while a subscription
// created with [OverflowDropNewest] opts out of that back pressure and accepts loss.
//
// Broadcaster starts no goroutines and is safe for concurrent use. Publishing never
// takes the lock that guards subscription bookkeeping, so a subscription created
// while Publish is running may miss that one item.
type Broadcaster[T any] struct {
	cfg config

	// mu serialises subscription bookkeeping and the copy-on-write snapshot that
	// Publish reads, which keeps Subscribe off the publish path.
	mu       sync.Mutex
	subs     []*Subscription[T] // ordered by registration
	snapshot atomic.Pointer[[]*Subscription[T]]
	nextID   uint64
	closed   atomic.Bool

	published atomic.Uint64
}

// NewBroadcaster returns a Broadcaster whose subscriptions inherit opts.
//
// Without options a subscription is unbuffered, so Publish waits until every
// subscription has received the item; [WithCapacity] gives each subscription room
// to absorb a burst, and [WithOverflow] decides what to do when that room runs out.
//
// NewBroadcaster panics on a negative capacity or an unknown overflow policy.
func NewBroadcaster[T any](opts ...Option) *Broadcaster[T] {
	b := &Broadcaster[T]{cfg: newConfig(opts, config{overflow: OverflowBlock})}
	empty := []*Subscription[T]{}
	b.snapshot.Store(&empty)
	return b
}

// Subscribe registers a new subscription, overriding the broadcaster's defaults
// with any options given.
//
// The subscription receives only the items published after it was registered. If
// the broadcaster is already closed, Subscribe still returns a usable subscription
// whose Next immediately reports [ErrClosed], so callers never have to special-case
// the shutdown race.
func (b *Broadcaster[T]) Subscribe(opts ...Option) *Subscription[T] {
	sub := &Subscription[T]{
		owner: b,
		buf:   newBuffer[T](newConfig(opts, b.cfg)),
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed.Load() {
		sub.buf.close()
		return sub
	}
	b.nextID++
	sub.id = b.nextID
	b.subs = append(b.subs, sub)
	b.publishSnapshotLocked()
	return sub
}

// Publish delivers item to every active subscription.
//
// Under [OverflowBlock] it waits until each subscription has room, so a slow
// consumer holds the producer back. If two subscriptions need room, Publish may
// already have delivered the item to the earlier ones when ctx is done, in which
// case it returns ctx.Err() after a partial fan-out. A subscription that is closed
// mid-publish simply forfeits its copy, and if the broadcaster itself is closed
// while Publish waits, Publish reports [ErrClosed].
func (b *Broadcaster[T]) Publish(ctx context.Context, item T) error {
	if b.closed.Load() {
		return ErrClosed
	}

	for _, sub := range *b.snapshot.Load() {
		err := sub.buf.push(ctx, item, sub.buf.overflow)
		if err == nil {
			continue
		}
		if !errors.Is(err, ErrClosed) {
			return fmt.Errorf("spmc: publish to subscription %d: %w", sub.id, err)
		}
		// The subscription unregistered itself while this item was in flight, or
		// the broadcaster is shutting down; only the latter affects the caller.
		if b.closed.Load() {
			return ErrClosed
		}
	}

	b.published.Add(1)
	return nil
}

// Close closes every subscription and refuses further items.
//
// Each subscription keeps its backlog, so subscribers drain what they already
// received and then observe [ErrClosed]. Close is idempotent.
func (b *Broadcaster[T]) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed.Swap(true) {
		return
	}
	subs := b.subs
	b.subs = nil
	b.publishSnapshotLocked()
	for _, sub := range subs {
		sub.buf.close()
	}
}

// Closed reports whether [Broadcaster.Close] has been called.
func (b *Broadcaster[T]) Closed() bool {
	return b.closed.Load()
}

// Subscribers returns the number of subscriptions currently registered.
func (b *Broadcaster[T]) Subscribers() int {
	return len(*b.snapshot.Load())
}

// Stats aggregates the broadcaster's own publish counter with the counters of the
// subscriptions registered at the time of the call.
//
// Subscriptions that were closed earlier no longer contribute, so a long-running
// service that replaces subscribers periodically should also scrape each
// subscription's own [Subscription.Stats] before closing it.
func (b *Broadcaster[T]) Stats() Stats {
	agg := Stats{Published: b.published.Load()}
	for _, sub := range *b.snapshot.Load() {
		s := sub.buf.stats()
		agg.Delivered += s.Delivered
		agg.Dropped += s.Dropped
		agg.Buffered += s.Buffered
	}
	return agg
}

// remove unregisters sub. It is called by [Subscription.Close] and tolerates a
// subscription that is no longer registered.
func (b *Broadcaster[T]) remove(sub *Subscription[T]) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for i, candidate := range b.subs {
		if candidate == sub {
			// The snapshot took its own copy, so editing b.subs in place cannot be
			// observed by an in-flight Publish.
			b.subs = append(b.subs[:i], b.subs[i+1:]...)
			break
		}
	}
	b.publishSnapshotLocked()
}

// publishSnapshotLocked republishes the immutable subscription slice that Publish
// reads without taking a lock. It must be called with b.mu held.
func (b *Broadcaster[T]) publishSnapshotLocked() {
	snapshot := make([]*Subscription[T], len(b.subs))
	copy(snapshot, b.subs)
	b.snapshot.Store(&snapshot)
}

// Subscription is one consumer's private view of a [Broadcaster].
//
// Each subscription owns a bounded buffer, so its pace, its overflow policy, and
// its backlog are independent of every other consumer. Subscription is safe for
// concurrent use; a typical consumer has one goroutine calling
// [Subscription.Next] and another calling [Subscription.Close].
type Subscription[T any] struct {
	owner *Broadcaster[T]
	id    uint64
	buf   *buffer[T]

	closeOnce sync.Once
}

// Next returns the next item for this subscription, blocking until one is
// available.
//
// It returns [ErrClosed] when the subscription, or its broadcaster, is closed and
// the subscription's backlog is drained, and ctx.Err() when the context is done
// first.
func (s *Subscription[T]) Next(ctx context.Context) (T, error) {
	return s.buf.pop(ctx)
}

// Items returns an iterator over the subscription, for use with a range loop:
//
//	for event := range sub.Items(ctx) {
//		handle(event)
//	}
//
// Iteration ends when the subscription closes and drains, when ctx is done, or when
// the loop body breaks.
func (s *Subscription[T]) Items(ctx context.Context) iter.Seq[T] {
	return iterate(s.Next, ctx)
}

// Close unregisters the subscription so the producer stops waiting for it.
//
// Items already buffered stay readable, so a consumer can drain what it has and
// then observe [ErrClosed]. Close is idempotent and safe to call concurrently with
// [Broadcaster.Publish].
func (s *Subscription[T]) Close() {
	s.closeOnce.Do(func() {
		s.buf.close()
		s.owner.remove(s)
	})
}

// Len returns the number of items waiting for this subscription.
func (s *Subscription[T]) Len() int {
	return s.buf.length()
}

// Drain removes and returns every item buffered for this subscription, in
// publication order.
//
// Like [Queue.Drain] it exists for shutdown paths that hand work to a different
// owner, and it counts the items as delivered.
func (s *Subscription[T]) Drain() []T {
	return s.buf.drain()
}

// Cap returns this subscription's capacity, or zero when it is unbuffered.
func (s *Subscription[T]) Cap() int {
	return s.buf.capacity()
}

// Stats returns a snapshot of this subscription's counters, which stay meaningful
// after the subscription is closed.
func (s *Subscription[T]) Stats() Stats {
	return s.buf.stats()
}
