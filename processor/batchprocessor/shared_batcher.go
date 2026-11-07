// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package batchprocessor // import "go.opentelemetry.io/collector/processor/batchprocessor"

import (
	"context"
	"errors"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"

	"go.opentelemetry.io/collector/client"
)

var errBatcherClosed = errors.New("batch processor is shutting down")

type sharedMetadataGroup[T any] struct {
	input     chan T
	exportCtx context.Context
}

// sharedMetadataBatcher shares processing workers, never admission buffers.
// A worker retains its tuple and builder until that builder becomes empty.
type sharedMetadataBatcher[T any] struct {
	processor     *batchProcessor[T]
	metadataKeys  []string
	metadataLimit int
	numShards     int

	mu      sync.Mutex
	groups  map[attribute.Set]*sharedMetadataGroup[T]
	order   []*sharedMetadataGroup[T]
	next    int
	changed chan struct{}
	closing bool
}

func (*sharedMetadataBatcher[T]) start(context.Context) error { return nil }

func (mb *sharedMetadataBatcher[T]) notify() {
	close(mb.changed)
	mb.changed = make(chan struct{})
}

func (mb *sharedMetadataBatcher[T]) close() {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	mb.closing = true
	mb.notify()
}

func (mb *sharedMetadataBatcher[T]) consume(ctx context.Context, data T) error {
	info := client.FromContext(ctx)
	attrs := make([]attribute.KeyValue, 0, len(mb.metadataKeys))
	for _, key := range mb.metadataKeys {
		values := info.Metadata.Get(key)
		if len(values) == 1 {
			attrs = append(attrs, attribute.String(key, values[0]))
		} else {
			attrs = append(attrs, attribute.StringSlice(key, values))
		}
	}
	key := attribute.NewSet(attrs...)
	fullRecorded := false
	for {
		mb.mu.Lock()
		if err := ctx.Err(); err != nil {
			mb.mu.Unlock()
			return err
		}
		if mb.closing {
			mb.mu.Unlock()
			return errBatcherClosed
		}
		group, exists := mb.groups[key]
		if !exists {
			if mb.metadataLimit != 0 && len(mb.groups) >= mb.metadataLimit {
				mb.mu.Unlock()
				return errTooManyBatchers
			}
			md := make(map[string][]string, len(mb.metadataKeys))
			for _, k := range mb.metadataKeys {
				md[k] = info.Metadata.Get(k)
			}
			group = &sharedMetadataGroup[T]{
				input: make(chan T, mb.processor.inputBufferSize),
				exportCtx: client.NewContext(context.Background(), client.Info{
					Metadata: client.NewMetadata(md),
				}),
			}
		}
		select {
		case group.input <- data:
			if !exists {
				mb.groups[key] = group
				mb.order = append(mb.order, group)
				for range mb.numShards {
					worker := &shard[T]{processor: mb.processor, batch: mb.processor.batchFunc()}
					mb.processor.goroutines.Go(func() { mb.run(worker) })
				}
			}
			mb.notify()
			mb.mu.Unlock()
			return nil
		default:
		}
		changed := mb.changed
		mb.mu.Unlock()
		if !fullRecorded {
			mb.processor.telemetry.recordBatcherFull()
			fullRecorded = true
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
}

type sharedWork uint8

const (
	sharedStop sharedWork = iota
	sharedItem
	sharedFlush
)

// take selects fairly among tuples for an empty worker. A partial builder can
// only receive its own tuple, or a flush signal. No data leaves a queue early.
func (mb *sharedMetadataBatcher[T]) take(bound *sharedMetadataGroup[T], tick <-chan time.Time) (*sharedMetadataGroup[T], T, sharedWork) {
	var zero T
	for {
		mb.mu.Lock()
		if bound != nil {
			if !mb.closing {
				select {
				case <-tick:
					mb.mu.Unlock()
					return bound, zero, sharedFlush
				default:
				}
			}
			select {
			case item := <-bound.input:
				mb.notify()
				mb.mu.Unlock()
				return bound, item, sharedItem
			default:
			}
		} else {
			for offset := range len(mb.order) {
				i := (mb.next + offset) % len(mb.order)
				group := mb.order[i]
				select {
				case item := <-group.input:
					mb.next = (i + 1) % len(mb.order)
					mb.notify()
					mb.mu.Unlock()
					return group, item, sharedItem
				default:
				}
			}
		}
		if mb.closing {
			mb.mu.Unlock()
			if bound != nil {
				return bound, zero, sharedFlush
			}
			return nil, zero, sharedStop
		}
		changed := mb.changed
		mb.mu.Unlock()
		select {
		case <-changed:
		case <-tick:
			return bound, zero, sharedFlush
		}
	}
}

func (mb *sharedMetadataBatcher[T]) run(worker *shard[T]) {
	var group *sharedMetadataGroup[T]
	var tick <-chan time.Time
	if mb.processor.timeout != 0 && mb.processor.sendBatchSize != 0 {
		worker.timer = time.NewTimer(mb.processor.timeout)
		tick = worker.timer.C
		defer worker.timer.Stop()
	}
	for {
		if worker.batch.itemCount() == 0 {
			group = nil
		}
		selected, item, action := mb.take(group, func() <-chan time.Time {
			if group == nil {
				return nil
			}
			return tick
		}())
		switch action {
		case sharedStop:
			return
		case sharedFlush:
			worker.sendItems(triggerTimeout)
			worker.resetTimer()
		case sharedItem:
			if group == nil {
				worker.bindExportContext(selected.exportCtx)
			}
			group = selected
			worker.processItem(item)
		}
	}
}

// bindExportContext switches an empty worker to a detached group context and
// starts that group's flush interval. It never derives from the previous context.
func (b *shard[T]) bindExportContext(ctx context.Context) {
	b.exportCtx = ctx
	b.stopTimer()
	b.resetTimer()
}

func (mb *sharedMetadataBatcher[T]) currentMetadataCardinality() int {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	return len(mb.groups)
}

func (mb *sharedMetadataBatcher[T]) currentShardCount() int {
	return mb.currentMetadataCardinality() * mb.numShards
}

func (mb *sharedMetadataBatcher[T]) currentQueueSize() int {
	mb.mu.Lock()
	defer mb.mu.Unlock()
	size := 0
	for _, group := range mb.order {
		size += len(group.input)
	}
	return size
}

func (mb *sharedMetadataBatcher[T]) currentQueueCapacity() int {
	return mb.currentMetadataCardinality() * mb.processor.inputBufferSize
}
