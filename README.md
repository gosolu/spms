# spmc

Single-producer/multiple-consumer (SPMC) primitives for Go: one producer hands work
to many consumers, with bounded buffers, explicit back pressure, and a graceful
shutdown.

The package depends on nothing outside the standard library and **starts no
goroutines of its own**. Consumers block inside `Next`, publishers block inside
`Publish`, and the caller keeps full control of the lifecycle. Go 1.23 or newer is
required (the iterators use `iter.Seq`).

Two shapes of SPMC communication are supported:

| | Type | Delivery |
| :--- | :--- | :--- |
| **Work distribution** | `Queue[T]` | Each item goes to exactly **one** consumer |
| **Fan-out** | `Broadcaster[T]` + `Subscription[T]` | Each item goes to **every** subscription |

## Install

```bash
go get github.com/gosolu/spmc
```

The module path is `github.com/gosolu/spmc`; adjust the `module` line in `go.mod`
if you fork it under a different repository.

## Work distribution

One producer, N consumers, each item processed once:

```go
ctx := context.Background()
queue := spmc.New[Job](spmc.WithCapacity(64))

var workers sync.WaitGroup
for range runtime.GOMAXPROCS(0) {
	workers.Add(1)
	go func() {
		defer workers.Done()
		for job := range queue.Items(ctx) {
			process(job)
		}
	}()
}

for _, job := range jobs {
	if err := queue.Publish(ctx, job); err != nil {
		return err
	}
}
queue.Close() // every buffered job is still delivered
workers.Wait()
```

Or without iterators, when a consumer needs to distinguish shutdown from
cancellation:

```go
for {
	job, err := queue.Next(ctx)
	if err != nil {
		if errors.Is(err, spmc.ErrClosed) {
			return nil // the producer finished and the queue is drained
		}
		return err // ctx.Err()
	}
	process(job)
}
```

## Fan-out

One producer, N independent consumers that each see every item:

```go
broadcaster := spmc.NewBroadcaster[Event](spmc.WithCapacity(256))

// One subscription per consumer, each with its own backlog.
fast := broadcaster.Subscribe()
lossy := broadcaster.Subscribe(
	spmc.WithCapacity(8),
	spmc.WithOverflow(spmc.OverflowDropNewest),
)

go func() {
	for event := range fast.Items(ctx) {
		handle(event)
	}
}()

for _, event := range events {
	if err := broadcaster.Publish(ctx, event); err != nil {
		return err
	}
}
broadcaster.Close()
```

A subscription can join or leave at any time. It receives only items published
after it joined, and `Close` removes it from the producer's path immediately while
whatever it already received stays readable.

## Back pressure

Every buffer is bounded. When it is full, the configured policy decides what
happens:

| Policy | Buffer full behaviour | Data loss | Publisher latency |
| :--- | :--- | :--- | :--- |
| `OverflowBlock` (default) | `Publish` waits for a free slot | none | unbounded |
| `OverflowDropNewest` | the new item is discarded, `Publish` returns `nil` | possible | none |
| `OverflowDropOldest` | the oldest buffered item is evicted | possible | none |
| `OverflowError` | `Publish` returns `ErrFull` | none | none |

`Queue.TryPublish` never blocks regardless of the configured policy; it returns
`ErrFull` when the item cannot be accepted immediately. Losses are always visible
in `Stats().Dropped`, so a dropping policy never fails silently.

A capacity of zero (the default) is **rendezvous mode**, matching an unbuffered Go
channel: `Publish` returns only after a consumer has taken the item, and on a queue
without a waiting consumer `TryPublish` returns `ErrFull`.

## Lifecycle

| Action | Effect |
| :--- | :--- |
| `Queue.Close` | stops accepting items; already buffered items stay readable, then `Next` returns `ErrClosed` |
| `Broadcaster.Close` | closes every subscription; each one drains its own backlog first |
| `Subscription.Close` | unregisters the subscription at once; its backlog stays readable |
| `Close` on a rendezvous item still pending | the item is discarded and its publisher receives `ErrClosed` |
| `Close` twice | no-op |

Because the package owns no goroutines, a consumer that stops consuming simply
stops: there is no leftover worker to leak, and `Close` is never required for
cleanup. `Close` exists to make shutdown deterministic, not to reclaim resources.

## Guarantees

- Each item is delivered to exactly one consumer (`Queue`) or to each subscription
  once (`Broadcaster`).
- Every consumer observes items in publication order; a consumer's stream is a
  subsequence of what the producer published.
- Safe for concurrent use, including several producers on the same queue. Ordering
  between producers is unspecified, which is consistent with the single-producer
  contract the API is designed around.
- A subscription that is slower than the producer cannot corrupt or slow down the
  others unless its policy is `OverflowBlock`, which is the point of that default.
- Cancelling a context affects only the caller that passed it: the item stays
  available to other consumers.

Not guaranteed: durability, cross-process delivery, inter-producer ordering, and
atomic all-or-nothing fan-out when a context is cancelled mid-`Publish` (earlier
subscriptions may already hold the item).

## API

| Type | Methods |
| :--- | :--- |
| `Queue[T]` | `Publish`, `TryPublish`, `Next`, `Items`, `Drain`, `Close`, `Closed`, `Len`, `Cap`, `Stats` |
| `Broadcaster[T]` | `Publish`, `Subscribe`, `Close`, `Closed`, `Subscribers`, `Stats` |
| `Subscription[T]` | `Next`, `Items`, `Drain`, `Close`, `Len`, `Cap`, `Stats` |
| Options | `WithCapacity`, `WithOverflow` |
| Errors | `ErrClosed`, `ErrFull` |

## Performance

`go test -bench . -benchmem`, Apple M5, Go 1.26, 200 ms per benchmark:

```text
BenchmarkQueue_PublishConsume/consumers=1-10     59.94 ns/op    101 B/op   0 allocs/op
BenchmarkQueue_PublishConsume/consumers=4-10     228.1 ns/op    100 B/op   0 allocs/op
BenchmarkQueue_PublishConsume/consumers=16-10    433.2 ns/op    217 B/op   1 allocs/op
BenchmarkQueue_Rendezvous-10                     331.7 ns/op    223 B/op   1 allocs/op
BenchmarkBroadcaster_Publish/subscribers=1-10    61.48 ns/op    105 B/op   0 allocs/op
BenchmarkBroadcaster_Publish/subscribers=4-10    406.3 ns/op    407 B/op   3 allocs/op
BenchmarkBroadcaster_Publish/subscribers=16-10  3024 ns/op    1769 B/op  15 allocs/op
```

The uncontended path allocates nothing. Under contention the only allocation is the
notification channel that a state change publishes to parked goroutines, which is
skipped entirely when nobody is waiting.

## Development

[`.github/workflows/ci.yml`](.github/workflows/ci.yml) runs the same gates as
`make check` on every push and pull request: `gofmt`, `go vet`, `golangci-lint`,
and `go test -race`.

```bash
make check   # fmt, vet, golangci-lint, go test -race
make cover   # coverage report
make bench   # benchmarks
```

The suite covers 100% of statements, including the concurrency paths, and runs with
`-race` by default in `make check`.
