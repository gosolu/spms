package spms

import (
	"context"
	"iter"
)

// Queue is a bounded FIFO that hands every published item to exactly one consumer,
// the single-producer/multiple-consumer work-distribution pattern.
//
// Any number of goroutines may call [Queue.Next], and each call receives a
// different item, so scaling consumers is a matter of running more of them. The
// intended usage is one producer goroutine calling [Queue.Publish]; more producers
// are safe but no ordering is defined between their items.
//
// Queue starts no goroutines. The producer owns the lifecycle: it calls
// [Queue.Close] when no further items will be published, and consumers keep
// receiving buffered items until the queue is drained, after which they observe
// [ErrClosed].
//
// Queue is safe for concurrent use by multiple goroutines.
//
// Example:
//
//	q := spms.New[Job](spms.WithCapacity(64))
//	var wg sync.WaitGroup
//	for range runtime.GOMAXPROCS(0) {
//		wg.Add(1)
//		go func() {
//			defer wg.Done()
//			for {
//				job, err := q.Next(ctx)
//				if err != nil {
//					return // ErrClosed: the producer is done and the queue drained.
//				}
//				process(job)
//			}
//		}()
//	}
//	for _, job := range jobs {
//		if err := q.Publish(ctx, job); err != nil {
//			log.Fatal(err)
//		}
//	}
//	q.Close()
//	wg.Wait()
type Queue[T any] struct {
	buf *buffer[T]
}

// New returns a Queue that hands each published item to exactly one consumer.
//
// Without options the queue is unbuffered, so Publish blocks until a consumer
// receives the item; [WithCapacity] trades that strict handoff for a backlog, and
// [WithOverflow] decides what happens when the backlog is full.
//
// New panics on a negative capacity or an unknown overflow policy.
func New[T any](opts ...Option) *Queue[T] {
	return &Queue[T]{buf: newBuffer[T](newConfig(opts, config{overflow: OverflowBlock}))}
}

// Publish offers item to the queue, where exactly one consumer will receive it.
//
// It blocks while the queue is full, unless [WithOverflow] selected a non-blocking
// policy, and returns [ErrClosed] once the queue is closed. When ctx is done first
// it returns ctx.Err(); a rendezvous item whose handoff is still pending then stays
// visible to consumers that are still draining, so treat that error as an unknown
// outcome.
func (q *Queue[T]) Publish(ctx context.Context, item T) error {
	return q.buf.push(ctx, item, q.buf.overflow)
}

// TryPublish offers item without ever blocking.
//
// It returns [ErrFull] when the item cannot be accepted immediately, which for a
// buffered queue means the buffer is full and for an unbuffered queue means no
// consumer is waiting. That makes TryPublish the right call on latency-sensitive
// paths where shedding load beats stalling the producer.
func (q *Queue[T]) TryPublish(item T) error {
	// OverflowError is the policy that refuses to wait, which is exactly the
	// contract of this method; the queue's own policy stays untouched.
	return q.buf.push(context.Background(), item, OverflowError)
}

// Next returns the next item, blocking until one is available.
//
// Exactly one consumer receives each item. Next returns [ErrClosed] once the queue
// is closed and drained, so a consumer loop that treats any error as "stop" shuts
// down cleanly. When ctx is done first, Next returns ctx.Err() and the item stays
// in the queue for another consumer.
func (q *Queue[T]) Next(ctx context.Context) (T, error) {
	return q.buf.pop(ctx)
}

// Items returns an iterator over the queue, for use with a range loop:
//
//	for job := range q.Items(ctx) {
//		process(job)
//	}
//
// Iteration ends when the queue is closed and drained, when ctx is done, or when
// the loop body breaks. Because the last two cases end iteration silently, callers
// that must distinguish them check ctx.Err() afterwards.
func (q *Queue[T]) Items(ctx context.Context) iter.Seq[T] {
	return iterate(q.Next, ctx)
}

// Drain removes and returns every buffered item in publication order.
//
// It exists for shutdown paths that hand work to a different owner. Call it after
// consumers have stopped, because the split of items between Drain and concurrent
// Next calls is otherwise arbitrary.
func (q *Queue[T]) Drain() []T {
	return q.buf.drain()
}

// Close stops accepting items. Buffered items stay available, so consumers drain
// the backlog and then observe [ErrClosed]; Close is idempotent.
func (q *Queue[T]) Close() {
	q.buf.close()
}

// Closed reports whether [Queue.Close] has been called.
func (q *Queue[T]) Closed() bool {
	return q.buf.isClosed()
}

// Len returns the number of items waiting to be consumed, which for an unbuffered
// queue is one while a handoff is in flight and zero otherwise.
func (q *Queue[T]) Len() int {
	return q.buf.length()
}

// Cap returns the configured capacity, or zero for an unbuffered queue.
func (q *Queue[T]) Cap() int {
	return q.buf.capacity()
}

// Stats returns a snapshot of the queue's counters.
func (q *Queue[T]) Stats() Stats {
	return q.buf.stats()
}

// iterate adapts a blocking receive method to an [iter.Seq] so that Queue and
// Subscription expose the same range-loop ergonomics without duplicating the loop.
func iterate[T any](next func(context.Context) (T, error), ctx context.Context) iter.Seq[T] {
	return func(yield func(T) bool) {
		for {
			item, err := next(ctx)
			if err != nil {
				return
			}
			if !yield(item) {
				return
			}
		}
	}
}
