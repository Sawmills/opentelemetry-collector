// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package batchprocessor

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/processor/batchprocessor/internal/metadata"
	"go.opentelemetry.io/collector/processor/processortest"
)

// This models the saved slow-chain sensitivity fixture, not a customer chain:
// one eight-second call, seventeen 1.4-second calls, and ten-second admission.
// Virtual time keeps the measured timing geometry without a slow wall-clock test.
func TestBusyMetadataUsesIdleWorkerWithoutMixingContexts(t *testing.T) {
	result := runMetadataAdmissionFixture(t, true, 1)
	assert.LessOrEqual(t, result.totalWorkers, 2, "processing worker budget must stay at two")
	assert.Equal(t, int64(2), result.hotWorkers, "the idle group's processing worker must serve the busy group")
	assert.GreaterOrEqual(t, result.accepted, 15, "the same eight-slot hot queue must sustain more admission than the serial baseline's eleven requests")
	assert.Less(t, result.initialDrain, 22*time.Second, "accepted work must drain faster, not just occupy a larger buffer")
}

func TestMetadataAdmissionSharingComparedWithFixedShards(t *testing.T) {
	if runtime.GOMAXPROCS(0) < 2 {
		t.Skip("fixed two-shard comparison requires at least two available processors")
	}
	serial := runMetadataAdmissionFixture(t, false, 1)
	fixed := runMetadataAdmissionFixture(t, false, 2)
	shared := runMetadataAdmissionFixture(t, true, 1)
	assert.Equal(t, 11, serial.accepted)
	assert.Equal(t, int64(1), serial.hotWorkers)
	assert.Equal(t, int64(2), fixed.hotWorkers)
	assert.Greater(t, fixed.accepted, serial.accepted, "existing sharding is also a possible configuration-only mitigation")
	assert.Greater(t, shared.accepted, serial.accepted)
	assert.Equal(t, 4, fixed.totalWorkers)
	assert.Equal(t, 2, shared.totalWorkers)
}

type admissionFixtureResult struct {
	accepted     int
	initialDrain time.Duration
	hotWorkers   int64
	totalWorkers int
}

func runMetadataAdmissionFixture(t *testing.T, share bool, numShards uint32) admissionFixtureResult {
	t.Helper()
	var result admissionFixtureResult
	synctest.Test(t, func(t *testing.T) {
		sink := &admissionLogsSink{ids: map[string]int{}, completed: make(chan struct{}, 64)}
		cfg := createDefaultConfig().(*Config)
		cfg.MetadataKeys = []string{"tenant", "route"}
		cfg.MetadataCardinalityLimit = 2
		cfg.NumShards = numShards
		cfg.ShareIdleWorkers = share
		cfg.Timeout = 0
		cfg.SendBatchSize = 500
		cfg.SendBatchMaxSize = 500
		logs, err := NewFactory().CreateLogs(context.Background(), processortest.NewNopSettings(metadata.Type), cfg, sink)
		require.NoError(t, err)
		// Inject host geometry before admission, rather than depending on CI CPUs.
		proc := logs.(*logsBatchProcessor)
		proc.inputBufferSize = 8
		require.NoError(t, logs.Start(context.Background(), componenttest.NewNopHost()))
		for _, tenant := range []string{"hot", "idle"} {
			require.NoError(t, logs.ConsumeLogs(admissionContext(tenant), admissionLogs(tenant+"/warmup", 1)))
		}
		sink.waitForRecords(2)
		require.Equal(t, 16, proc.batcher.currentQueueCapacity())
		require.Equal(t, 2*int(numShards), proc.batcher.currentShardCount())

		const requests = 18
		type rejectedRequest struct {
			data plog.Logs
			err  error
		}
		results := make([]rejectedRequest, requests)
		var calls sync.WaitGroup
		start := time.Now()
		for i := range requests {
			data := admissionLogs(fmt.Sprintf("hot/burst-%02d", i), 500)
			calls.Go(func() {
				ctx, cancel := context.WithDeadline(admissionContext("hot"), start.Add(10*time.Second))
				defer cancel()
				if err := logs.ConsumeLogs(ctx, data); err != nil {
					results[i] = rejectedRequest{data: data, err: err}
				}
			})
		}
		calls.Wait()
		accepted := requests
		for _, r := range results {
			if r.err != nil {
				accepted--
				assert.ErrorIs(t, r.err, context.DeadlineExceeded)
				assert.Equal(t, 500, r.data.LogRecordCount(), "rejected admission must leave the caller's request intact")
			}
		}
		sink.waitForRecords(2 + accepted*500)
		initialDrain := time.Since(start)
		// Retry only rejected requests after accepted work drains. Reuse their
		// original data to exercise the caller's retained ownership contract.
		for _, r := range results {
			if r.err != nil {
				require.NoError(t, logs.ConsumeLogs(admissionContext("hot"), r.data))
			}
		}
		sink.waitForRecords(2 + requests*500)
		require.NoError(t, logs.Shutdown(context.Background()))
		require.Empty(t, sink.contextErrors)
		require.Len(t, sink.ids, 2+requests*500)
		for _, tenant := range []string{"hot", "idle"} {
			require.Equal(t, 1, sink.ids[tenant+"/warmup/000"])
		}
		for request := range requests {
			for record := range 500 {
				id := fmt.Sprintf("hot/burst-%02d/%03d", request, record)
				require.Equal(t, 1, sink.ids[id], "missing or duplicate delivery of %s", id)
			}
		}
		result = admissionFixtureResult{accepted: accepted, initialDrain: initialDrain, hotWorkers: sink.hotMax.Load(), totalWorkers: proc.batcher.currentShardCount()}
		t.Logf("sharing=%t shards_per_group=%d accepted=%d/%d initial_drain=%s hot_workers=%d total_workers=%d", share, numShards, accepted, requests, initialDrain, result.hotWorkers, result.totalWorkers)
		assert.LessOrEqual(t, sink.max.Load(), int64(result.totalWorkers), "processing concurrency must stay within the existing worker budget")
	})
	return result
}

func TestDefaultOffKeepsSerialMetadataOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var order []string
		var mu sync.Mutex
		var active, peak atomic.Int64
		sink, err := consumer.NewLogs(func(_ context.Context, data plog.Logs) error {
			admissionUpdateMax(&peak, active.Add(1))
			defer active.Add(-1)
			time.Sleep(time.Second)
			mu.Lock()
			order = append(order, data.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Body().Str())
			mu.Unlock()
			return nil
		})
		require.NoError(t, err)
		cfg := createDefaultConfig().(*Config)
		require.False(t, cfg.ShareIdleWorkers)
		cfg.MetadataKeys = []string{"tenant"}
		cfg.Timeout = 0
		logs, err := NewFactory().CreateLogs(context.Background(), processortest.NewNopSettings(metadata.Type), cfg, sink)
		require.NoError(t, err)
		require.NoError(t, logs.Start(context.Background(), componenttest.NewNopHost()))
		for i := range 6 {
			require.NoError(t, logs.ConsumeLogs(admissionContext("hot"), admissionLogs(fmt.Sprintf("hot/%d", i), 1)))
		}
		require.NoError(t, logs.Shutdown(context.Background()))
		require.Equal(t, []string{"hot/0/000", "hot/1/000", "hot/2/000", "hot/3/000", "hot/4/000", "hot/5/000"}, order)
		require.Equal(t, int64(1), peak.Load())
	})
}

func TestSharedWorkersServeColdGroupBeforeHotBacklog(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		started := make(chan string, 20)
		release := make(chan struct{})
		sink, err := consumer.NewLogs(func(_ context.Context, data plog.Logs) error {
			id := data.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Body().Str()
			started <- id
			if strings.HasPrefix(id, "hot/") {
				<-release
			}
			return nil
		})
		require.NoError(t, err)
		logs := newSharedAdmissionProcessor(t, sink, 0, 1)
		require.NoError(t, logs.ConsumeLogs(admissionContext("idle"), admissionLogs("idle/warmup", 1)))
		require.Equal(t, "idle/warmup/000", <-started)
		for i := range 6 {
			require.NoError(t, logs.ConsumeLogs(admissionContext("hot"), admissionLogs(fmt.Sprintf("hot/%d", i), 1)))
		}
		<-started
		<-started
		require.NoError(t, logs.ConsumeLogs(admissionContext("idle"), admissionLogs("idle/new", 1)))
		release <- struct{}{}
		require.Equal(t, "idle/new/000", <-started, "a newly runnable cold tuple must precede more hot work")
		close(release)
		require.NoError(t, logs.Shutdown(context.Background()))
	})
}

func newSharedAdmissionProcessor(t *testing.T, sink consumer.Logs, timeout time.Duration, batchSize uint32) *logsBatchProcessor {
	t.Helper()
	cfg := createDefaultConfig().(*Config)
	cfg.MetadataKeys = []string{"tenant", "route"}
	cfg.MetadataCardinalityLimit = 2
	cfg.NumShards = 1
	cfg.ShareIdleWorkers = true
	cfg.Timeout = timeout
	cfg.SendBatchSize = batchSize
	logs, err := NewFactory().CreateLogs(context.Background(), processortest.NewNopSettings(metadata.Type), cfg, sink)
	require.NoError(t, err)
	proc := logs.(*logsBatchProcessor)
	proc.inputBufferSize = 8
	require.NoError(t, proc.Start(context.Background(), componenttest.NewNopHost()))
	return proc
}

func admissionContext(tenant string) context.Context {
	return client.NewContext(context.Background(), client.Info{Metadata: client.NewMetadata(map[string][]string{
		"tenant": {tenant}, "route": {"route-" + tenant}, "unselected": {"must-not-propagate"},
	})})
}

func admissionLogs(prefix string, count int) plog.Logs {
	data := plog.NewLogs()
	records := data.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
	for i := range count {
		records.AppendEmpty().Body().SetStr(fmt.Sprintf("%s/%03d", prefix, i))
	}
	return data
}

type admissionLogsSink struct {
	mu            sync.Mutex
	ids           map[string]int
	contextErrors []string
	completed     chan struct{}
	firstHot      atomic.Bool
	active        atomic.Int64
	max           atomic.Int64
	hotActive     atomic.Int64
	hotMax        atomic.Int64
}

func (*admissionLogsSink) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (s *admissionLogsSink) ConsumeLogs(ctx context.Context, data plog.Logs) error {
	active := s.active.Add(1)
	defer s.active.Add(-1)
	admissionUpdateMax(&s.max, active)
	md := client.FromContext(ctx).Metadata
	tenant := strings.Join(md.Get("tenant"), ",")
	var ids []string
	var contextErrors []string
	for _, rl := range data.ResourceLogs().All() {
		for _, sl := range rl.ScopeLogs().All() {
			for _, record := range sl.LogRecords().All() {
				id := record.Body().Str()
				ids = append(ids, id)
				if !strings.HasPrefix(id, tenant+"/") || strings.Join(md.Get("route"), ",") != "route-"+tenant || len(md.Get("unselected")) != 0 {
					contextErrors = append(contextErrors, id)
				}
			}
		}
	}
	if len(ids) > 0 && strings.HasPrefix(ids[0], "hot/burst-") {
		admissionUpdateMax(&s.hotMax, s.hotActive.Add(1))
		defer s.hotActive.Add(-1)
		if s.firstHot.CompareAndSwap(false, true) {
			time.Sleep(8 * time.Second)
		} else {
			time.Sleep(1400 * time.Millisecond)
		}
	}
	s.mu.Lock()
	for _, id := range ids {
		s.ids[id]++
	}
	s.contextErrors = append(s.contextErrors, contextErrors...)
	s.mu.Unlock()
	s.completed <- struct{}{}
	return nil
}

func (s *admissionLogsSink) waitForRecords(count int) {
	for {
		s.mu.Lock()
		n := len(s.ids)
		s.mu.Unlock()
		if n >= count {
			return
		}
		<-s.completed
	}
}

func admissionUpdateMax(maximum *atomic.Int64, value int64) {
	for {
		old := maximum.Load()
		if value <= old || maximum.CompareAndSwap(old, value) {
			return
		}
	}
}
