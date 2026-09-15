// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package batchprocessor

import (
	"context"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/testdata"
	"go.opentelemetry.io/collector/pdata/xpdata/pref"
	"go.opentelemetry.io/collector/processor/batchprocessor/internal/metadata"
	"go.opentelemetry.io/collector/processor/batchprocessor/internal/metadatatest"
	"go.opentelemetry.io/collector/processor/processortest"
)

func TestCanceledAdmissionPreservesInputAndReleasesReference(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cfg := createDefaultConfig().(*Config)
	set := processortest.NewNopSettings(metadata.Type)

	t.Run("logs", func(t *testing.T) {
		p, err := newLogsBatchProcessor(set, consumertest.NewNop(), cfg)
		require.NoError(t, err)
		t.Cleanup(func() { assert.NoError(t, p.Shutdown(context.WithoutCancel(ctx))) })
		data := testdata.GenerateLogs(1)
		require.ErrorIs(t, p.ConsumeLogs(ctx, data), context.Canceled)
		require.Equal(t, 1, data.LogRecordCount())
		pref.UnrefLogs(data)
		assert.PanicsWithValue(t, "Cannot unref freed data", func() { pref.UnrefLogs(data) })
	})
	t.Run("metrics", func(t *testing.T) {
		p, err := newMetricsBatchProcessor(set, consumertest.NewNop(), cfg)
		require.NoError(t, err)
		t.Cleanup(func() { assert.NoError(t, p.Shutdown(context.WithoutCancel(ctx))) })
		data := testdata.GenerateMetrics(1)
		count := data.DataPointCount()
		require.ErrorIs(t, p.ConsumeMetrics(ctx, data), context.Canceled)
		require.Equal(t, count, data.DataPointCount())
		pref.UnrefMetrics(data)
		assert.PanicsWithValue(t, "Cannot unref freed data", func() { pref.UnrefMetrics(data) })
	})
	t.Run("traces", func(t *testing.T) {
		p, err := newTracesBatchProcessor(set, consumertest.NewNop(), cfg)
		require.NoError(t, err)
		t.Cleanup(func() { assert.NoError(t, p.Shutdown(context.WithoutCancel(ctx))) })
		data := testdata.GenerateTraces(1)
		require.ErrorIs(t, p.ConsumeTraces(ctx, data), context.Canceled)
		require.Equal(t, 1, data.SpanCount())
		pref.UnrefTraces(data)
		assert.PanicsWithValue(t, "Cannot unref freed data", func() { pref.UnrefTraces(data) })
	})
}

func TestCanceledAdmissionUnderBurst(t *testing.T) {
	for _, tc := range []struct {
		name         string
		shards       uint32
		metadataKeys []string
	}{
		{name: "single", shards: 1},
		{name: "fixed", shards: 2},
		{name: "metadata_single", shards: 1, metadataKeys: []string{"tenant"}},
		{name: "metadata_sharded", shards: 2, metadataKeys: []string{"tenant"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if int(tc.shards) > runtime.GOMAXPROCS(0) {
				t.Skip("test requires at least two available processors")
			}
			tel := componenttest.NewTelemetry()
			t.Cleanup(func() { assert.NoError(t, tel.Shutdown(context.Background())) })
			sink := &blockingLogsSink{started: make(chan struct{}, tc.shards), release: make(chan struct{})}
			cfg := createDefaultConfig().(*Config)
			cfg.NumShards = tc.shards
			cfg.MetadataKeys = tc.metadataKeys
			cfg.Timeout = 0
			cfg.SendBatchSize = 1
			p, err := newLogsBatchProcessor(metadatatest.NewSettings(tel), sink, cfg)
			require.NoError(t, err)
			require.NoError(t, p.Start(context.Background(), componenttest.NewNopHost()))
			var pending sync.WaitGroup
			release := sync.OnceFunc(func() { close(sink.release) })
			shutdown := sync.OnceFunc(func() { assert.NoError(t, p.Shutdown(context.Background())) })
			t.Cleanup(func() {
				release()
				pending.Wait()
				shutdown()
			})
			ctx := client.NewContext(context.Background(), client.Info{
				Metadata: client.NewMetadata(map[string][]string{"tenant": {"tenant-a"}}),
			})
			for range tc.shards {
				require.NoError(t, p.ConsumeLogs(ctx, testdata.GenerateLogs(1)))
			}
			for range tc.shards {
				select {
				case <-sink.started:
				case <-time.After(time.Second):
					t.Fatal("batch shard did not reach the sink")
				}
			}
			for range runtime.NumCPU() {
				require.NoError(t, p.ConsumeLogs(ctx, testdata.GenerateLogs(1)))
			}
			bp := p.(*logsBatchProcessor)
			require.Equal(t, bp.batcher.currentQueueCapacity(), bp.batcher.currentQueueSize())
			const burstSize = 16
			results := make(chan error, burstSize)
			cancelCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			for range burstSize {
				pending.Go(func() { results <- p.ConsumeLogs(cancelCtx, testdata.GenerateLogs(1)) })
			}
			require.Eventually(t, func() bool {
				metric, metricErr := tel.GetMetric("otelcol_processor_batch_batcher_full")
				if metricErr != nil {
					return false
				}
				sum, ok := metric.Data.(metricdata.Sum[int64])
				return ok && len(sum.DataPoints) == 1 && sum.DataPoints[0].Value == burstSize
			}, time.Second, time.Millisecond)
			cancel()
			for range burstSize {
				select {
				case err := <-results:
					require.ErrorIs(t, err, context.Canceled)
					require.False(t, consumererror.IsPermanent(err))
				case <-time.After(time.Second):
					t.Fatal("canceled admission is still blocked on the full queue")
				}
			}
			require.Equal(t, bp.batcher.currentQueueCapacity(), bp.batcher.currentQueueSize())
			release()
			for range burstSize {
				require.NoError(t, p.ConsumeLogs(ctx, testdata.GenerateLogs(1)))
			}
			shutdown()
			require.Equal(t, int64(int(tc.shards)+runtime.NumCPU()+burstSize), sink.count.Load())
		})
	}
}
