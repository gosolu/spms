package spmc

import (
	"context"
	"fmt"
	"sync"
	"testing"
)

func BenchmarkQueue_PublishConsume(b *testing.B) {
	for _, consumers := range []int{1, 4, 16} {
		b.Run(fmt.Sprintf("consumers=%d", consumers), func(b *testing.B) {
			ctx := context.Background()
			q := New[int](WithCapacity(1024))

			var workers sync.WaitGroup
			for range consumers {
				workers.Add(1)
				go func() {
					defer workers.Done()
					for {
						if _, err := q.Next(ctx); err != nil {
							return
						}
					}
				}()
			}

			b.ReportAllocs()
			b.ResetTimer()
			for item := 0; item < b.N; item++ {
				if err := q.Publish(ctx, item); err != nil {
					b.Fatalf("Publish: %v", err)
				}
			}
			b.StopTimer()

			q.Close()
			workers.Wait()
		})
	}
}

// BenchmarkQueue_Rendezvous measures the strict hand-off mode, where every publish
// waits for a consumer to take the item and no buffer absorbs the cost.
func BenchmarkQueue_Rendezvous(b *testing.B) {
	ctx := context.Background()
	q := New[int]()

	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			if _, err := q.Next(ctx); err != nil {
				return
			}
		}
	}()

	b.ReportAllocs()
	b.ResetTimer()
	for item := 0; item < b.N; item++ {
		if err := q.Publish(ctx, item); err != nil {
			b.Fatalf("Publish: %v", err)
		}
	}
	b.StopTimer()

	q.Close()
	<-done
}

func BenchmarkBroadcaster_Publish(b *testing.B) {
	for _, subscribers := range []int{1, 4, 16} {
		b.Run(fmt.Sprintf("subscribers=%d", subscribers), func(b *testing.B) {
			ctx := context.Background()
			broadcaster := NewBroadcaster[int](WithCapacity(1024))

			var workers sync.WaitGroup
			for range subscribers {
				sub := broadcaster.Subscribe()
				workers.Add(1)
				go func() {
					defer workers.Done()
					for {
						if _, err := sub.Next(ctx); err != nil {
							return
						}
					}
				}()
			}

			b.ReportAllocs()
			b.ResetTimer()
			for item := 0; item < b.N; item++ {
				if err := broadcaster.Publish(ctx, item); err != nil {
					b.Fatalf("Publish: %v", err)
				}
			}
			b.StopTimer()

			broadcaster.Close()
			workers.Wait()
		})
	}
}
