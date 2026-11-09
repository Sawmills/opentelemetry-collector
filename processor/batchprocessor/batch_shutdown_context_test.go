// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package batchprocessor_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"go.opentelemetry.io/collector/client"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/processor/batchprocessor"
	"go.opentelemetry.io/collector/processor/processortest"
)

func TestShutdownContextCancelsInFlightMetricsHTTPRequest(t *testing.T) {
	for _, mode := range []struct {
		name     string
		shards   uint32
		metadata bool
		shared   bool
	}{
		{name: "single", shards: 1},
		{name: "fixed", shards: 2},
		{name: "metadata", shards: 1, metadata: true},
		{name: "metadata fixed", shards: 2, metadata: true},
		{name: "shared metadata", shards: 2, metadata: true, shared: true},
	} {
		t.Run(mode.name, func(t *testing.T) {
			cfg := batchprocessor.NewFactory().CreateDefaultConfig().(*batchprocessor.Config)
			cfg.Timeout = 0
			cfg.SendBatchSize = 1
			cfg.NumShards = mode.shards
			if mode.metadata {
				cfg.MetadataKeys = []string{"tenant"}
			}
			cfg.ShareIdleWorkers = mode.shared
			testShutdownContextCancelsMetricsHTTPRequest(t, cfg, false)
		})
	}
}

func TestShutdownContextCancelsBufferedFinalMetricsHTTPRequest(t *testing.T) {
	cfg := batchprocessor.NewFactory().CreateDefaultConfig().(*batchprocessor.Config)
	cfg.Timeout = time.Hour
	cfg.SendBatchSize = 100
	cfg.MetadataKeys = []string{"tenant"}
	cfg.ShareIdleWorkers = true
	testShutdownContextCancelsMetricsHTTPRequest(t, cfg, true)
}

func TestShutdownContextAlreadyCanceledReachesFinalExport(t *testing.T) {
	seen := make(chan error, 1)
	next, err := consumer.NewMetrics(func(ctx context.Context, _ pmetric.Metrics) error {
		seen <- ctx.Err()
		return ctx.Err()
	})
	require.NoError(t, err)
	factory := batchprocessor.NewFactory()
	cfg := factory.CreateDefaultConfig().(*batchprocessor.Config)
	cfg.Timeout = time.Hour
	metrics, err := factory.CreateMetrics(t.Context(), processortest.NewNopSettings(component.MustNewType("batch")), cfg, next)
	require.NoError(t, err)
	require.NoError(t, metrics.Start(t.Context(), componenttest.NewNopHost()))
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	shutdown := sync.OnceValue(func() error { return metrics.Shutdown(ctx) })
	t.Cleanup(func() { _ = shutdown() })
	require.NoError(t, metrics.ConsumeMetrics(t.Context(), shutdownContextMetric()))
	shutdownErr := shutdown()
	select {
	case exportErr := <-seen:
		require.ErrorIs(t, exportErr, context.Canceled, "already-canceled cleanup reached final export with a live context")
	case <-time.After(time.Second):
		t.Fatal("accepted metric did not reach final export")
	}
	require.ErrorIs(t, shutdownErr, context.Canceled)
}

func TestShutdownContextPreservesUncanceledDrainAndComponentLifetime(t *testing.T) {
	release := make(chan struct{})
	releaseExport := sync.OnceFunc(func() { close(release) })
	t.Cleanup(releaseExport)
	seen := make(chan context.Context, 1)
	next, err := consumer.NewMetrics(func(ctx context.Context, _ pmetric.Metrics) error {
		seen <- ctx
		<-release
		return nil
	})
	require.NoError(t, err)
	factory := batchprocessor.NewFactory()
	cfg := factory.CreateDefaultConfig().(*batchprocessor.Config)
	cfg.Timeout = 0
	metrics, err := factory.CreateMetrics(t.Context(), processortest.NewNopSettings(component.MustNewType("batch")), cfg, next)
	require.NoError(t, err)
	startCtx, cancelStart := context.WithCancel(t.Context())
	defer cancelStart()
	require.NoError(t, metrics.Start(startCtx, componenttest.NewNopHost()))
	shutdown := sync.OnceValue(func() error { return metrics.Shutdown(context.Background()) })
	t.Cleanup(func() { releaseExport(); _ = shutdown() })
	consumeCtx, cancelConsume := context.WithCancel(t.Context())
	defer cancelConsume()
	require.NoError(t, metrics.ConsumeMetrics(consumeCtx, shutdownContextMetric()))
	var exportCtx context.Context
	select {
	case exportCtx = <-seen:
	case <-time.After(time.Second):
		t.Fatal("accepted metric did not reach export")
	}
	cancelStart()
	cancelConsume()
	require.NoError(t, exportCtx.Err(), "short-lived Start and Consume contexts must not cancel owned batch work")
	done := make(chan error, 1)
	go func() { done <- shutdown() }()
	select {
	case err := <-done:
		t.Fatalf("uncanceled Shutdown returned before export acknowledgement: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	require.NoError(t, exportCtx.Err())
	releaseExport()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("uncanceled Shutdown did not finish after acknowledgement")
	}
}

func TestShutdownContextWaitsForContextIgnoringConsumer(t *testing.T) {
	release := make(chan struct{})
	releaseExport := sync.OnceFunc(func() { close(release) })
	t.Cleanup(releaseExport)
	entered := make(chan struct{})
	next, err := consumer.NewMetrics(func(context.Context, pmetric.Metrics) error {
		close(entered)
		<-release
		return nil
	})
	require.NoError(t, err)
	factory := batchprocessor.NewFactory()
	cfg := factory.CreateDefaultConfig().(*batchprocessor.Config)
	cfg.Timeout = 0
	metrics, err := factory.CreateMetrics(t.Context(), processortest.NewNopSettings(component.MustNewType("batch")), cfg, next)
	require.NoError(t, err)
	require.NoError(t, metrics.Start(t.Context(), componenttest.NewNopHost()))
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	shutdown := sync.OnceValue(func() error { return metrics.Shutdown(ctx) })
	t.Cleanup(func() { releaseExport(); _ = shutdown() })
	require.NoError(t, metrics.ConsumeMetrics(t.Context(), shutdownContextMetric()))
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("accepted metric did not reach export")
	}
	done := make(chan error, 1)
	go func() { done <- shutdown() }()
	<-ctx.Done()
	select {
	case err := <-done:
		t.Fatalf("Shutdown returned while the consumer still owned work: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	releaseExport()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.DeadlineExceeded)
	case <-time.After(time.Second):
		t.Fatal("Shutdown did not join the released consumer")
	}
}

func shutdownContextMetric() pmetric.Metrics {
	data := pmetric.NewMetrics()
	metric := data.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	metric.SetName("owned.batch.shutdown")
	metric.SetEmptyGauge().DataPoints().AppendEmpty().SetIntValue(1)
	return data
}

func testShutdownContextCancelsMetricsHTTPRequest(t *testing.T, cfg *batchprocessor.Config, flushOnShutdown bool) {
	t.Helper()
	release := make(chan struct{})
	releaseResponse := sync.OnceFunc(func() { close(release) })
	type observation struct {
		points int
		tenant string
	}
	received := make(chan observation, 1)
	canceled := make(chan struct{})
	var peers atomic.Int64
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		_ = r.Body.Close()
		request := pmetricotlp.NewExportRequest()
		if err != nil || request.UnmarshalProto(body) != nil {
			received <- observation{points: -1}
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		received <- observation{points: request.Metrics().DataPointCount(), tenant: r.Header.Get("X-Owned-Tenant")}
		select {
		case <-release:
			w.WriteHeader(http.StatusOK)
		case <-r.Context().Done():
			close(canceled)
		}
	}))
	server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		switch state {
		case http.StateNew:
			peers.Add(1)
		case http.StateClosed, http.StateHijacked:
			peers.Add(-1)
		}
	}
	server.Start()
	t.Cleanup(server.Close)
	t.Cleanup(releaseResponse)
	httpClient := server.Client()
	next, err := consumer.NewMetrics(func(ctx context.Context, data pmetric.Metrics) error {
		body, marshalErr := pmetricotlp.NewExportRequestFromMetrics(data).MarshalProto()
		if marshalErr != nil {
			return marshalErr
		}
		request, requestErr := http.NewRequestWithContext(ctx, http.MethodPost, server.URL, bytes.NewReader(body))
		if requestErr != nil {
			return requestErr
		}
		request.Header.Set("Content-Type", "application/x-protobuf")
		request.Header["X-Owned-Tenant"] = client.FromContext(ctx).Metadata.Get("tenant")
		response, sendErr := httpClient.Do(request)
		if sendErr != nil {
			return sendErr
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusOK {
			return fmt.Errorf("unexpected metrics response: %d", response.StatusCode)
		}
		return nil
	})
	require.NoError(t, err)
	factory := batchprocessor.NewFactory()
	metrics, err := factory.CreateMetrics(t.Context(), processortest.NewNopSettings(component.MustNewType("batch")), cfg, next)
	require.NoError(t, err)
	require.NoError(t, metrics.Start(t.Context(), componenttest.NewNopHost()))
	shutdownStarted := false
	t.Cleanup(func() {
		releaseResponse()
		if !shutdownStarted {
			_ = metrics.Shutdown(context.Background())
		}
	})
	data := pmetric.NewMetrics()
	metric := data.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	metric.SetName("owned.batch.shutdown")
	metric.SetEmptyGauge().DataPoints().AppendEmpty().SetIntValue(1)
	consumeCtx := client.NewContext(t.Context(), client.Info{Metadata: client.NewMetadata(map[string][]string{"tenant": {"owned-tenant"}})})
	require.NoError(t, metrics.ConsumeMetrics(consumeCtx, data))
	waitRequest := func() {
		select {
		case seen := <-received:
			require.Equal(t, 1, seen.points, "the public batch adapter must enter the real OTLP request")
			if len(cfg.MetadataKeys) > 0 {
				require.Equal(t, "owned-tenant", seen.tenant)
			}
		case <-time.After(time.Second):
			t.Fatal("batch export did not reach the owned HTTP server")
		}
	}
	if !flushOnShutdown {
		waitRequest()
	}
	var ctx context.Context
	var cancel context.CancelFunc
	var wantError error = context.DeadlineExceeded
	if flushOnShutdown {
		ctx, cancel = context.WithCancel(t.Context())
		wantError = context.Canceled
	} else {
		ctx, cancel = context.WithTimeout(t.Context(), 50*time.Millisecond)
	}
	defer cancel()
	done := make(chan error, 1)
	shutdownStarted = true
	go func() { done <- metrics.Shutdown(ctx) }()
	if flushOnShutdown {
		waitRequest()
		cancel()
	}
	select {
	case shutdownErr := <-done:
		require.ErrorIs(t, shutdownErr, wantError)
	case <-time.After(time.Second):
		releaseResponse()
		select {
		case shutdownErr := <-done:
			t.Fatalf("Shutdown ignored its caller deadline; returned %v only after the same reply was released", shutdownErr)
		case <-time.After(time.Second):
			t.Fatal("batch retained its worker after the owned response was released")
		}
	}
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("Shutdown returned before canceling the actual downstream request")
	}
	require.Eventually(t, func() bool { return peers.Load() == 0 }, time.Second, time.Millisecond,
		"Shutdown must release its downstream peer before the fixture releases the reply")
}
