// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package batchprocessor

import (
	"context"
	"errors"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/processor/batchprocessor/internal/metadata"
	"go.opentelemetry.io/collector/processor/processortest"
)

type admissionLogServer struct {
	plogotlp.UnimplementedGRPCServer
	next      consumer.Logs
	entered   chan struct{}
	completed chan error
}

func (s *admissionLogServer) Export(ctx context.Context, request plogotlp.ExportRequest) (plogotlp.ExportResponse, error) {
	s.entered <- struct{}{}
	err := s.next.ConsumeLogs(ctx, request.Logs())
	s.completed <- err
	if err != nil {
		return plogotlp.NewExportResponse(), status.FromContextError(err).Err()
	}
	return plogotlp.NewExportResponse(), nil
}

func TestGRPCDeadlineRejectsAdmissionBeforeRetry(t *testing.T) {
	const numShards = 2
	if runtime.GOMAXPROCS(0) < numShards {
		t.Skip("test requires at least two available processors")
	}
	started := make(chan struct{}, numShards)
	releaseC := make(chan struct{})
	release := sync.OnceFunc(func() { close(releaseC) })
	sink := new(consumertest.LogsSink)
	exportErrors := make(chan error, numShards+runtime.NumCPU()+1)
	next, err := consumer.NewLogs(func(ctx context.Context, data plog.Logs) error {
		select {
		case started <- struct{}{}:
		default:
		}
		<-releaseC
		exportErrors <- ctx.Err()
		return sink.ConsumeLogs(ctx, data)
	})
	require.NoError(t, err)
	cfg := createDefaultConfig().(*Config)
	cfg.NumShards = numShards
	cfg.Timeout = 0
	cfg.SendBatchSize = 1
	p, err := newLogsBatchProcessor(processortest.NewNopSettings(metadata.Type), next, cfg)
	require.NoError(t, err)
	require.NoError(t, p.Start(context.Background(), componenttest.NewNopHost()))
	shutdown := sync.OnceFunc(func() { assert.NoError(t, p.Shutdown(context.Background())) })
	t.Cleanup(func() { release(); shutdown() })
	record := func(id string) plog.Logs {
		data := plog.NewLogs()
		data.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr(id)
		return data
	}
	acceptedCtx, acceptedCancel := context.WithCancel(context.Background())
	defer acceptedCancel()
	for range numShards {
		require.NoError(t, p.ConsumeLogs(acceptedCtx, record("accepted")))
	}
	for range numShards {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("batch shard did not reach the sink")
		}
	}
	for range runtime.NumCPU() {
		require.NoError(t, p.ConsumeLogs(acceptedCtx, record("accepted")))
	}
	acceptedCancel()

	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	handler := &admissionLogServer{next: p, entered: make(chan struct{}, 2), completed: make(chan error, 2)}
	plogotlp.RegisterGRPCServer(server, handler)
	serveDone := make(chan struct{})
	go func() { defer close(serveDone); _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); assert.NoError(t, listener.Close()); <-serveDone })
	cc, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, cc.Close()) })
	client := plogotlp.NewGRPCClient(cc)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err = client.Export(ctx, plogotlp.NewExportRequestFromLogs(record("retry")))
	require.Equal(t, codes.DeadlineExceeded, status.Code(err))
	select {
	case <-handler.entered:
	default:
		t.Fatal("the timed-out request never reached the server")
	}
	select {
	case admissionErr := <-handler.completed:
		require.Error(t, admissionErr)
		require.True(t, errors.Is(admissionErr, context.Canceled) || errors.Is(admissionErr, context.DeadlineExceeded))
	case <-time.After(time.Second):
		t.Fatal("server admission is still blocked after the client deadline")
	}
	release()
	retryCtx, retryCancel := context.WithTimeout(context.Background(), time.Second)
	defer retryCancel()
	_, err = client.Export(retryCtx, plogotlp.NewExportRequestFromLogs(record("retry")))
	require.NoError(t, err)
	shutdown()
	close(exportErrors)
	for exportErr := range exportErrors {
		require.NoError(t, exportErr, "accepted batches retain an independent export context")
	}
	counts := make(map[string]int)
	for _, data := range sink.AllLogs() {
		for _, resource := range data.ResourceLogs().All() {
			for _, scope := range resource.ScopeLogs().All() {
				for _, item := range scope.LogRecords().All() {
					counts[item.Body().Str()]++
				}
			}
		}
	}
	require.Equal(t, map[string]int{"accepted": numShards + runtime.NumCPU(), "retry": 1}, counts)
}
