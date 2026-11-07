// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package batchprocessor

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/metric/metricdata/metricdatatest"

	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/processor/batchprocessor/internal/metadatatest"
)

func TestSharedAdmissionBoundsCancellationAndShutdown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		tel := componenttest.NewTelemetry()
		defer func() { require.NoError(t, tel.Shutdown(context.Background())) }()
		started := make(chan struct{}, 20)
		contextErrors := make(chan error, 20)
		release := make(chan struct{})
		var mu sync.Mutex
		ids := map[string]int{}
		sink, err := consumer.NewLogs(func(ctx context.Context, data plog.Logs) error {
			started <- struct{}{}
			<-release
			if contextErr := ctx.Err(); contextErr != nil {
				contextErrors <- contextErr
			}
			id := data.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Body().Str()
			mu.Lock()
			ids[id]++
			mu.Unlock()
			return nil
		})
		require.NoError(t, err)
		cfg := createDefaultConfig().(*Config)
		cfg.MetadataKeys = []string{"tenant", "route"}
		cfg.MetadataCardinalityLimit = 2
		cfg.ShareIdleWorkers = true
		cfg.Timeout = 0
		logs, err := NewFactory().CreateLogs(context.Background(), metadatatest.NewSettings(tel), cfg, sink)
		require.NoError(t, err)
		logs.(*logsBatchProcessor).inputBufferSize = 8
		require.NoError(t, logs.Start(context.Background(), componenttest.NewNopHost()))
		for _, tenant := range []string{"hot", "idle"} {
			ctx, cancel := context.WithCancel(admissionContext(tenant))
			require.NoError(t, logs.ConsumeLogs(ctx, admissionLogs(tenant+"/active", 1)))
			<-started
			cancel()
		}
		for i := range 8 {
			require.NoError(t, logs.ConsumeLogs(admissionContext("hot"), admissionLogs(fmt.Sprintf("hot/queued-%d", i), 1)))
		}
		ctx, cancel := context.WithTimeout(admissionContext("hot"), time.Second)
		defer cancel()
		rejected := admissionLogs("hot/rejected", 1)
		result := make(chan error, 1)
		go func() { result <- logs.ConsumeLogs(ctx, rejected) }()
		synctest.Wait()
		select {
		case admissionErr := <-result:
			t.Fatalf("hot admission borrowed the idle queue: %v", admissionErr)
		default:
		}
		for i := range 8 {
			require.NoError(t, logs.ConsumeLogs(admissionContext("idle"), admissionLogs(fmt.Sprintf("idle/queued-%d", i), 1)))
			synctest.Wait()
		}
		attrs := attribute.NewSet(attribute.String("processor", "batch"))
		metadatatest.AssertEqualProcessorBatchQueueSize(t, tel, []metricdata.DataPoint[int64]{
			{Attributes: attrs, Value: 16},
		}, metricdatatest.IgnoreTimestamp())
		metadatatest.AssertEqualProcessorBatchQueueCapacity(t, tel, []metricdata.DataPoint[int64]{
			{Attributes: attrs, Value: 16},
		}, metricdatatest.IgnoreTimestamp())
		metadatatest.AssertEqualProcessorBatchActiveShards(t, tel, []metricdata.DataPoint[int64]{
			{Attributes: attrs, Value: 2},
		}, metricdatatest.IgnoreTimestamp())
		metadatatest.AssertEqualProcessorBatchMetadataCardinality(t, tel, []metricdata.DataPoint[int64]{
			{Attributes: attrs, Value: 2},
		}, metricdatatest.IgnoreTimestamp())
		// A full hot queue cannot borrow the idle group's eight admission slots.
		time.Sleep(time.Second)
		require.ErrorIs(t, <-result, context.DeadlineExceeded)
		require.Equal(t, 1, rejected.LogRecordCount())
		metadatatest.AssertEqualProcessorBatchBatcherFull(t, tel, []metricdata.DataPoint[int64]{
			{Attributes: attrs, Value: 1},
		}, metricdatatest.IgnoreTimestamp())
		third := admissionLogs("third/rejected", 1)
		err = logs.ConsumeLogs(admissionContext("third"), third)
		require.True(t, consumererror.IsPermanent(err))
		require.Equal(t, 1, third.LogRecordCount())
		canceled, cancelNow := context.WithCancel(admissionContext("third"))
		cancelNow()
		require.ErrorIs(t, logs.ConsumeLogs(canceled, third), context.Canceled)

		blocked := admissionLogs("hot/blocked-at-shutdown", 1)
		go func() { result <- logs.ConsumeLogs(admissionContext("hot"), blocked) }()
		synctest.Wait()
		shutdown := make(chan error, 1)
		go func() { shutdown <- logs.Shutdown(context.Background()) }()
		synctest.Wait()
		require.ErrorIs(t, <-result, errBatcherClosed)
		require.Equal(t, 1, blocked.LogRecordCount())
		require.ErrorIs(t, logs.ConsumeLogs(admissionContext("hot"), rejected), errBatcherClosed)
		require.Equal(t, 1, rejected.LogRecordCount())
		close(release)
		require.NoError(t, <-shutdown)
		require.Empty(t, contextErrors, "accepted work must not inherit caller cancellation")
		require.Len(t, ids, 18)
		for _, tenant := range []string{"hot", "idle"} {
			require.Equal(t, 1, ids[tenant+"/active/000"])
			for i := range 8 {
				require.Equal(t, 1, ids[fmt.Sprintf("%s/queued-%d/000", tenant, i)])
			}
		}
		metadatatest.AssertEqualProcessorBatchQueueSize(t, tel, []metricdata.DataPoint[int64]{
			{Attributes: attrs, Value: 0},
		}, metricdatatest.IgnoreTimestamp())
	})
}

func TestSharedPartialBatchTimerKeepsMetadataAndDrainsOnShutdown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sink := &admissionLogsSink{ids: map[string]int{}, completed: make(chan struct{}, 64)}
		logs := newSharedAdmissionProcessor(t, sink, time.Second, 10)
		for _, tenant := range []string{"hot", "idle"} {
			require.NoError(t, logs.ConsumeLogs(admissionContext(tenant), admissionLogs(tenant+"/timed", 1)))
		}
		synctest.Wait()
		sink.mu.Lock()
		beforeTimeout := len(sink.ids)
		sink.mu.Unlock()
		require.Zero(t, beforeTimeout, "partial batches must wait for the timer")
		time.Sleep(time.Second)
		sink.waitForRecords(2)
		for _, tenant := range []string{"hot", "idle"} {
			require.NoError(t, logs.ConsumeLogs(admissionContext(tenant), admissionLogs(tenant+"/shutdown", 1)))
		}
		require.NoError(t, logs.Shutdown(context.Background()))
		require.Empty(t, sink.contextErrors)
		require.Equal(t, map[string]int{
			"hot/timed/000": 1, "idle/timed/000": 1,
			"hot/shutdown/000": 1, "idle/shutdown/000": 1,
		}, sink.ids)
	})
}

func TestSharedDownstreamFailureIsNotRetriedByBatchProcessor(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var mu sync.Mutex
		attempts := map[string]int{}
		sink, err := consumer.NewLogs(func(_ context.Context, data plog.Logs) error {
			mu.Lock()
			defer mu.Unlock()
			id := data.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Body().Str()
			attempts[id]++
			return errors.New("downstream owns any retry after accepted admission")
		})
		require.NoError(t, err)
		logs := newSharedAdmissionProcessor(t, sink, 0, 1)
		for _, tenant := range []string{"hot", "idle"} {
			require.NoError(t, logs.ConsumeLogs(admissionContext(tenant), admissionLogs(tenant+"/failed", 1)))
		}
		require.NoError(t, logs.Shutdown(context.Background()))
		require.Equal(t, map[string]int{"hot/failed/000": 1, "idle/failed/000": 1}, attempts)
	})
}

func TestSharedWorkersPermitSameMetadataCompletionOutOfOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		started := make(chan string, 3)
		completed := make(chan string, 3)
		release := make(chan struct{})
		sink, err := consumer.NewLogs(func(_ context.Context, data plog.Logs) error {
			id := data.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Body().Str()
			started <- id
			if id == "hot/first/000" {
				<-release
			}
			completed <- id
			return nil
		})
		require.NoError(t, err)
		logs := newSharedAdmissionProcessor(t, sink, 0, 1)
		require.NoError(t, logs.ConsumeLogs(admissionContext("idle"), admissionLogs("idle/warmup", 1)))
		<-started
		<-completed
		require.NoError(t, logs.ConsumeLogs(admissionContext("hot"), admissionLogs("hot/first", 1)))
		require.Equal(t, "hot/first/000", <-started)
		require.NoError(t, logs.ConsumeLogs(admissionContext("hot"), admissionLogs("hot/second", 1)))
		require.Equal(t, "hot/second/000", <-completed)
		close(release)
		require.NoError(t, logs.Shutdown(context.Background()))
		require.Equal(t, "hot/first/000", <-completed)
	})
}
