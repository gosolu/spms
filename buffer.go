package spms

import (
	"context"
	"sync"
	"sync/atomic"
)

// buffer is the bounded FIFO engine behind [Queue] and [Subscription].
//
// It never starts a goroutine: a caller that cannot make progress parks on signal,
// a channel that is closed and replaced whenever the buffer changes state.
// Capturing that channel under mu and closing it under the same mutex makes a lost
// wakeup impossible, and replacing it forces every woken caller to re-inspect the
// state instead of trusting a snapshot that may already be stale.
//
// In every method, "Locked" means that the caller already holds mu.
type buffer[T any] struct {
	// mu guards the fields below except the counters, which are only ever touched
	// with atomics so that Stats never contends with the data path.
	mu sync.Mutex

	// ring is the circular storage of buffered mode. It is nil in rendezvous mode
	// and, once constructed, never resized, so it may be read without holding mu.
	ring []T

	head int // index of the oldest item in ring
	size int // number of items held in ring

	handoff    T      // rendezvous mode: the item offered to the next consumer
	hasHandoff bool   // rendezvous mode: handoff holds a valid item
	offers     uint64 // rendezvous mode: monotonic counter of offers
	handoffSeq uint64 // rendezvous mode: sequence of the item in the slot, 0 if empty
	discarded  uint64 // rendezvous mode: sequence of the offer a close discarded
	closed     bool

	signal  chan struct{}
	waiters int

	overflow Overflow

	published atomic.Uint64
	delivered atomic.Uint64
	dropped   atomic.Uint64
}

// newBuffer returns a buffer configured by cfg.
func newBuffer[T any](cfg config) *buffer[T] {
	b := &buffer[T]{
		signal:   make(chan struct{}),
		overflow: cfg.overflow,
	}
	if cfg.capacity > 0 {
		b.ring = make([]T, cfg.capacity)
	}
	return b
}

// rendezvous reports whether the buffer hands items over directly instead of
// holding them, as an unbuffered channel does.
func (b *buffer[T]) rendezvous() bool {
	return b.ring == nil
}

// capacity returns the number of items the buffer holds; zero means rendezvous.
func (b *buffer[T]) capacity() int {
	return len(b.ring)
}

// length returns the number of items waiting to be consumed.
func (b *buffer[T]) length() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.lengthLocked()
}

// lengthLocked reports the backlog, including an item whose handoff is pending.
func (b *buffer[T]) lengthLocked() int {
	if b.hasHandoff {
		return b.size + 1
	}
	return b.size
}

// isClosed reports whether the buffer has been closed.
func (b *buffer[T]) isClosed() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.closed
}

// stats returns a snapshot of the counters and of the live backlog.
//
// The counters are read atomically and may advance between reads, which is why
// Stats documents itself as a point-in-time view rather than a transaction.
func (b *buffer[T]) stats() Stats {
	return Stats{
		Published: b.published.Load(),
		Delivered: b.delivered.Load(),
		Dropped:   b.dropped.Load(),
		Buffered:  b.length(),
	}
}

// push enqueues item according to policy.
//
// It returns ErrClosed when the buffer is closed, ErrFull when policy is
// OverflowError and no room is available, and ctx.Err() when the context is done
// first. Under a dropping policy it returns nil even though the item may have been
// discarded, so callers that must know consult [Stats].Dropped.
func (b *buffer[T]) push(ctx context.Context, item T, policy Overflow) error {
	if b.rendezvous() {
		return b.pushRendezvous(ctx, item, policy)
	}

	b.mu.Lock()
	for {
		if b.closed {
			b.mu.Unlock()
			return ErrClosed
		}
		if b.size < len(b.ring) {
			b.enqueueLocked(item)
			// A consumer may be parked waiting for work.
			b.wakeLocked()
			b.mu.Unlock()
			b.published.Add(1)
			return nil
		}
		switch policy {
		case OverflowDropNewest:
			b.dropped.Add(1)
			b.mu.Unlock()
			return nil
		case OverflowDropOldest:
			b.dropOldestLocked()
			b.enqueueLocked(item)
			b.wakeLocked()
			b.mu.Unlock()
			b.published.Add(1)
			return nil
		case OverflowError:
			b.mu.Unlock()
			return ErrFull
		default:
			// OverflowBlock: park until a consumer frees a slot. waitLocked
			// re-acquires the lock, so the loop re-evaluates every condition.
			if err := b.waitLocked(ctx); err != nil {
				b.mu.Unlock()
				return err
			}
		}
	}
}

// pushRendezvous implements publishing for an unbuffered buffer, mirroring a send
// on an unbuffered channel: the item is accepted only once a consumer takes it.
//
// The producer waits for its own handoff to be resolved, which is what makes the
// outcome of Publish unambiguous. A close resolves a pending handoff as ErrClosed
// and discards the item, because after shutdown nobody is obliged to receive it.
func (b *buffer[T]) pushRendezvous(ctx context.Context, item T, policy Overflow) error {
	b.mu.Lock()

	if b.closed {
		b.mu.Unlock()
		return ErrClosed
	}

	if policy != OverflowBlock {
		// A non-blocking publish on an unbuffered buffer can only succeed when a
		// consumer is already parked, exactly like a send in a select that has a
		// default branch.
		if b.hasHandoff || b.waiters == 0 {
			b.mu.Unlock()
			return b.rejectOverflow(policy)
		}
		b.offerLocked(item)
		b.mu.Unlock()
		return nil
	}

	// Serialise with a previous handoff that no consumer has taken yet: the single
	// slot is the whole buffer, so the newest item waits its turn.
	for b.hasHandoff {
		if err := b.waitLocked(ctx); err != nil {
			b.mu.Unlock()
			return err
		}
		if b.closed {
			b.mu.Unlock()
			return ErrClosed
		}
	}

	seq := b.offerLocked(item)
	for {
		if b.handoffSeq != seq {
			// The slot no longer holds this publisher's item: a consumer took it,
			// or a close discarded it. Matching on the offer's sequence is what
			// keeps the outcome unambiguous when another publisher refills the
			// slot before this goroutine is scheduled again.
			discarded := b.discarded == seq
			b.mu.Unlock()
			if discarded {
				return ErrClosed
			}
			return nil
		}
		// A cancelled context leaves the item visible to consumers that are still
		// draining, so the caller must treat ctx.Err() as an unknown outcome.
		if err := b.waitLocked(ctx); err != nil {
			b.mu.Unlock()
			return err
		}
	}
}

// offerLocked publishes item into the rendezvous slot, wakes a consumer, and
// returns the sequence number that identifies this offer.
func (b *buffer[T]) offerLocked(item T) uint64 {
	b.offers++
	b.handoff = item
	b.hasHandoff = true
	b.handoffSeq = b.offers
	b.published.Add(1)
	b.wakeLocked()
	return b.handoffSeq
}

// rejectOverflow applies a policy that is not allowed to wait for room.
func (b *buffer[T]) rejectOverflow(policy Overflow) error {
	if policy == OverflowError {
		return ErrFull
	}
	// DropNewest and DropOldest behave alike here: with no backlog to evict, the
	// newcomer is the item that goes.
	b.dropped.Add(1)
	return nil
}

// pop returns the next item, blocking until one is available.
//
// It returns ErrClosed once the buffer is closed and empty, and ctx.Err() when the
// context is done first.
func (b *buffer[T]) pop(ctx context.Context) (T, error) {
	b.mu.Lock()
	for {
		if item, ok := b.dequeueLocked(); ok {
			// Handing an item over frees room for a parked producer.
			b.wakeLocked()
			b.mu.Unlock()
			b.delivered.Add(1)
			return item, nil
		}
		if b.closed {
			b.mu.Unlock()
			var zero T
			return zero, ErrClosed
		}
		if err := b.waitLocked(ctx); err != nil {
			b.mu.Unlock()
			var zero T
			return zero, err
		}
	}
}

// drain removes every buffered item and returns them in publication order.
func (b *buffer[T]) drain() []T {
	b.mu.Lock()
	defer b.mu.Unlock()

	backlog := b.lengthLocked()
	if backlog == 0 {
		return nil
	}
	out := make([]T, 0, backlog)
	for {
		item, ok := b.dequeueLocked()
		if !ok {
			break
		}
		out = append(out, item)
		b.delivered.Add(1)
	}
	b.wakeLocked()
	return out
}

// close marks the buffer as closed.
//
// Items already buffered stay readable, which is what lets consumers drain the
// backlog before they observe ErrClosed. A rendezvous handoff that no consumer has
// taken is discarded instead: after shutdown nobody is obliged to receive it, and
// the producer must not be told that an undelivered item was accepted.
func (b *buffer[T]) close() {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return
	}
	b.closed = true
	if b.hasHandoff {
		b.hasHandoff = false
		b.discarded = b.handoffSeq
		b.handoffSeq = 0
		var zero T
		b.handoff = zero
		b.dropped.Add(1)
	}
	b.wakeLocked()
}

// enqueueLocked writes item at the tail of the ring.
func (b *buffer[T]) enqueueLocked(item T) {
	idx := b.head + b.size
	if idx >= len(b.ring) {
		idx -= len(b.ring)
	}
	b.ring[idx] = item
	b.size++
}

// dequeueLocked removes the oldest item, reporting whether one was available.
func (b *buffer[T]) dequeueLocked() (T, bool) {
	if b.hasHandoff {
		item := b.handoff
		var zero T
		b.handoff = zero
		b.hasHandoff = false
		b.handoffSeq = 0
		return item, true
	}
	if b.size == 0 {
		var zero T
		return zero, false
	}

	item := b.ring[b.head]
	// Clear the vacated slot so a long-lived ring cannot pin payloads after they
	// were handed over.
	var zero T
	b.ring[b.head] = zero
	b.head++
	if b.head == len(b.ring) {
		b.head = 0
	}
	b.size--
	return item, true
}

// dropOldestLocked evicts the oldest buffered item and counts the loss.
func (b *buffer[T]) dropOldestLocked() {
	if _, ok := b.dequeueLocked(); ok {
		b.dropped.Add(1)
	}
}

// waitLocked releases mu, blocks until the buffer changes state or ctx is done, and
// returns with mu held again. Callers must re-check their condition afterwards,
// because a wakeup may come from an unrelated state change.
func (b *buffer[T]) waitLocked(ctx context.Context) error {
	// Register before releasing the lock: a state change from here on closes and
	// replaces signal, so the wakeup cannot be lost in the window between checking
	// the condition and parking.
	b.waiters++
	sig := b.signal
	b.mu.Unlock()

	var err error
	select {
	case <-sig:
	case <-ctx.Done():
		err = ctx.Err()
	}

	b.mu.Lock()
	b.waiters--
	return err
}

// wakeLocked wakes every parked caller, if there is one.
func (b *buffer[T]) wakeLocked() {
	if b.waiters == 0 {
		// Allocating a channel for every state change would put an allocation on
		// the hot path even when nobody is parked.
		return
	}
	close(b.signal)
	b.signal = make(chan struct{})
}
