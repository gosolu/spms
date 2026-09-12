package spms

import "errors"

// ErrClosed reports that a queue, broadcaster, or subscription is closed and has
// nothing left to hand over.
//
// [Queue.Next], [Queue.Publish], [Broadcaster.Publish], and [Subscription.Next]
// all return it; use [errors.Is] rather than comparing error values directly,
// because errors travelling across a subscription are wrapped with context.
var ErrClosed = errors.New("spms: closed")

// ErrFull reports that an item could not be accepted because the destination
// buffer was full and the configured [Overflow] policy was [OverflowError].
//
// It is also returned by [Queue.TryPublish] whenever an item cannot be accepted
// without waiting.
var ErrFull = errors.New("spms: buffer full")
