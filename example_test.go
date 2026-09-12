package spmc_test

import (
	"context"
	"fmt"
	"log"
	"slices"
	"sync"

	"github.com/gosolu/spmc"
)

// ExampleQueue shows the work-distribution shape: one producer, one consumer, and
// a graceful shutdown in which the consumer drains the backlog before it stops.
func ExampleQueue() {
	ctx := context.Background()
	// The capacity gives the producer room to run the whole batch ahead of the
	// consumer; with the default unbuffered queue each Publish would wait for a
	// matching Next.
	queue := spmc.New[int](spmc.WithCapacity(3))

	for job := 1; job <= 3; job++ {
		if err := queue.Publish(ctx, job); err != nil {
			log.Fatal(err)
		}
	}
	queue.Close()

	for {
		job, err := queue.Next(ctx)
		if err != nil {
			break // spmc.ErrClosed: the producer is done and the queue is drained.
		}
		fmt.Println("processed", job)
	}

	// Output:
	// processed 1
	// processed 2
	// processed 3
}

// ExampleQueue_multipleConsumers scales the same queue across several consumers:
// every job is processed exactly once, and the order of completion varies.
func ExampleQueue_multipleConsumers() {
	ctx := context.Background()
	queue := spmc.New[int](spmc.WithCapacity(4))

	var (
		mu       sync.Mutex
		finished []int
		workers  sync.WaitGroup
	)
	for range 4 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for job := range queue.Items(ctx) {
				mu.Lock()
				finished = append(finished, job)
				mu.Unlock()
			}
		}()
	}

	for job := 1; job <= 8; job++ {
		if err := queue.Publish(ctx, job); err != nil {
			log.Fatal(err)
		}
	}
	queue.Close()
	workers.Wait()

	// Consumers complete in an unpredictable order, so sort before printing.
	slices.Sort(finished)
	fmt.Println(finished)

	// Output: [1 2 3 4 5 6 7 8]
}

// ExampleQueue_Items iterates the queue with a range loop, which ends when the
// producer closes the queue and the last item has been consumed.
func ExampleQueue_Items() {
	ctx := context.Background()
	queue := spmc.New[string](spmc.WithCapacity(1))

	go func() {
		for _, value := range []string{"a", "b", "c"} {
			if err := queue.Publish(ctx, value); err != nil {
				log.Fatal(err)
			}
		}
		queue.Close()
	}()

	for value := range queue.Items(ctx) {
		fmt.Println(value)
	}

	// Output:
	// a
	// b
	// c
}

// ExampleBroadcaster shows the fan-out shape: two independent consumers of the
// same stream, each with its own backlog.
func ExampleBroadcaster() {
	ctx := context.Background()
	// Each subscription gets room for the whole batch, so the producer never has to
	// wait for a consumer to catch up.
	broadcaster := spmc.NewBroadcaster[int](spmc.WithCapacity(3))

	first := broadcaster.Subscribe()
	second := broadcaster.Subscribe()

	for event := 1; event <= 3; event++ {
		if err := broadcaster.Publish(ctx, event); err != nil {
			log.Fatal(err)
		}
	}
	broadcaster.Close()

	for event := range first.Items(ctx) {
		fmt.Println("first", event)
	}
	for event := range second.Items(ctx) {
		fmt.Println("second", event)
	}

	// Output:
	// first 1
	// first 2
	// first 3
	// second 1
	// second 2
	// second 3
}

// ExampleQueue_TryPublish sheds load instead of stalling the producer, which is
// what a latency-sensitive publisher wants once consumers fall behind.
func ExampleQueue_TryPublish() {
	queue := spmc.New[int](spmc.WithCapacity(1))

	fmt.Println(queue.TryPublish(1))
	fmt.Println(queue.TryPublish(2))
	fmt.Println("buffered:", queue.Len(), "dropped:", queue.Stats().Dropped)

	// Output:
	// <nil>
	// spmc: buffer full
	// buffered: 1 dropped: 0
}
