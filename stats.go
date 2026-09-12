package spms

// Stats is a point-in-time view of a buffer's counters.
//
// The counters are monotonically increasing, while Buffered describes the live
// backlog and goes up and down with traffic. Because the counters are read
// independently they are a snapshot, not a transaction.
type Stats struct {
	// Published counts the items accepted from the producer. A call whose item was
	// discarded by OverflowDropNewest adds nothing here, and a call that returned
	// ErrFull adds nothing anywhere, so Published + Dropped never double counts.
	Published uint64

	// Delivered counts the items handed over to a consumer, either through Next,
	// through Drain, or as a broadcast copy.
	Delivered uint64

	// Dropped counts the items silently discarded by an overflow policy, including
	// a rendezvous item whose handoff was pending when the buffer closed.
	Dropped uint64

	// Buffered is the number of items waiting to be consumed right now.
	Buffered int
}
