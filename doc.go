// Package spms provides single-producer/multiple-consumer (SPMC) primitives for
// Go. It depends on nothing outside the standard library and starts no goroutines
// of its own, so every lifecycle decision stays with the caller.
//
// Two shapes of SPMC communication are supported.
//
// [Queue] is the work-distribution shape in which a single producer publishes an
// item and exactly one of any number of consumers receives it:
//
//	q := spms.New[Job](spms.WithCapacity(64))
//	// ... N consumer goroutines calling q.Next(ctx) ...
//	// ... one producer goroutine calling q.Publish(ctx, job) ...
//	q.Close() // consumers drain the backlog and then observe spms.ErrClosed
//
// [Broadcaster] is the fan-out shape in which a single producer publishes an item
// and every registered [Subscription] receives its own copy:
//
//	b := spms.NewBroadcaster[Event](spms.WithCapacity(16))
//	sub := b.Subscribe() // one per consumer
//	// ... b.Publish(ctx, event) ...
//	sub.Close() // this consumer stops; the others keep receiving
//
// # Lifecycle
//
// Closing is graceful in both cases: items that were already accepted stay
// readable, so consumers drain the backlog and only then receive [ErrClosed].
// Because the package owns no goroutines, a caller that stops consuming simply
// stops; there is nothing to leak.
//
// # Back pressure
//
// Every buffer is bounded. When a consumer falls behind, [OverflowBlock] (the
// default) pushes back on the producer instead of growing without limit, while
// [OverflowDropNewest], [OverflowDropOldest], and [OverflowError] trade
// completeness for latency. A capacity of zero is rendezvous mode, matching the
// semantics of an unbuffered Go channel: publishing succeeds only once a consumer
// takes the item.
package spms
