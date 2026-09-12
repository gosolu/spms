package spms

import "fmt"

// Overflow defines how a buffer reacts when it is full and the producer tries to
// publish another item.
type Overflow int

const (
	// OverflowBlock makes Publish wait until a consumer frees a slot. It is the
	// default: no item is ever lost, and a slow consumer throttles the producer.
	OverflowBlock Overflow = iota

	// OverflowDropNewest discards the item being published and returns nil, which
	// keeps the producer at full speed at the cost of completeness. Dropped items
	// are counted in [Stats].Dropped.
	OverflowDropNewest

	// OverflowDropOldest discards the oldest buffered item to make room for the new
	// one. It suits streams such as telemetry, where freshness matters more than
	// completeness. Dropped items are counted in [Stats].Dropped.
	OverflowDropOldest

	// OverflowError makes Publish return [ErrFull] immediately instead of blocking
	// or losing data, leaving the decision to the caller.
	OverflowError
)

// String returns the policy name, which makes policy values readable in logs and
// test failure messages.
func (o Overflow) String() string {
	switch o {
	case OverflowBlock:
		return "block"
	case OverflowDropNewest:
		return "drop-newest"
	case OverflowDropOldest:
		return "drop-oldest"
	case OverflowError:
		return "error"
	default:
		return fmt.Sprintf("Overflow(%d)", int(o))
	}
}

// Option configures a [Queue], a [Broadcaster], or a single [Subscription].
//
// The same option set applies to all three so that a subscription inherits the
// broadcaster's defaults and only overrides what it needs:
//
//	b := spms.NewBroadcaster[Event](spms.WithCapacity(1024))
//	lossy := b.Subscribe(spms.WithCapacity(8), spms.WithOverflow(spms.OverflowDropNewest))
type Option func(*config)

// config holds the resolved settings shared by every buffer in the package.
type config struct {
	capacity int
	overflow Overflow
}

// newConfig applies opts on top of base and validates the result.
//
// Invalid settings panic rather than degrade silently: a mistyped capacity or
// policy is a programming error that would otherwise surface much later as
// unexplained back pressure or data loss.
func newConfig(opts []Option, base config) config {
	cfg := base
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	if cfg.capacity < 0 {
		panic(fmt.Sprintf("spms: negative capacity %d", cfg.capacity))
	}
	if cfg.overflow < OverflowBlock || cfg.overflow > OverflowError {
		panic(fmt.Sprintf("spms: invalid overflow policy %d", int(cfg.overflow)))
	}
	return cfg
}

// WithCapacity sets how many items a buffer holds before its overflow policy
// applies.
//
// A capacity of zero, the default, selects rendezvous mode: the producer's
// Publish call returns only after a consumer has taken the item, exactly like a
// send on an unbuffered channel. Any positive capacity decouples the producer
// from short consumer stalls. WithCapacity panics on a negative capacity, as
// make(chan T, n) does.
func WithCapacity(n int) Option {
	return func(c *config) {
		c.capacity = n
	}
}

// WithOverflow selects the policy applied when the buffer is full.
//
// The default is [OverflowBlock]. Values outside the defined policies are
// rejected with a panic.
func WithOverflow(policy Overflow) Option {
	return func(c *config) {
		c.overflow = policy
	}
}
