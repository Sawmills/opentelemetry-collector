// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package queue

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/metric/metricdata/metricdatatest"

	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/exporter/exporterhelper/internal/metadatatest"
	"go.opentelemetry.io/collector/exporter/exporterhelper/internal/request"
	"go.opentelemetry.io/collector/exporter/exporterhelper/internal/requesttest"
	"go.opentelemetry.io/collector/pipeline"
)

func TestObsQueueLogsSuccessExposesZeroFailures(t *testing.T) {
	telemetry := componenttest.NewTelemetry()
	t.Cleanup(func() { require.NoError(t, telemetry.Shutdown(context.Background())) })
	obs, err := newObsQueue[request.Request](Settings[request.Request]{
		Signal: pipeline.SignalLogs, ID: exporterID, Telemetry: telemetry.NewTelemetrySettings(),
	}, newFakeQueue[request.Request](nil, 0, 10))
	require.NoError(t, err)
	require.NoError(t, obs.Offer(context.Background(), &requesttest.FakeRequest{Items: 5}))
	metadatatest.AssertEqualExporterEnqueueFailedLogRecords(t, telemetry,
		[]metricdata.DataPoint[int64]{{Attributes: attribute.NewSet(attribute.String(exporterKey, exporterID.String())), Value: 0}},
		metricdatatest.IgnoreTimestamp(), metricdatatest.IgnoreExemplars())
}

func TestObsQueueLogsEmptyOfferDoesNotInventFailureCoverage(t *testing.T) {
	telemetry := componenttest.NewTelemetry()
	t.Cleanup(func() { require.NoError(t, telemetry.Shutdown(context.Background())) })
	obs, err := newObsQueue[request.Request](Settings[request.Request]{
		Signal: pipeline.SignalLogs, ID: exporterID, Telemetry: telemetry.NewTelemetrySettings(),
	}, newFakeQueue[request.Request](nil, 0, 10))
	require.NoError(t, err)
	_, err = telemetry.GetMetric("otelcol_exporter_enqueue_failed_log_records")
	require.Error(t, err)
	require.NoError(t, obs.Offer(context.Background(), &requesttest.FakeRequest{}))
	_, err = telemetry.GetMetric("otelcol_exporter_enqueue_failed_log_records")
	require.Error(t, err)
}

func TestObsQueueLogsFailureAfterSuccessCountsEachRejectedItem(t *testing.T) {
	telemetry := componenttest.NewTelemetry()
	t.Cleanup(func() { require.NoError(t, telemetry.Shutdown(context.Background())) })
	delegate := &fakeQueue[request.Request]{capacity: 10}
	obs, err := newObsQueue[request.Request](Settings[request.Request]{
		Signal: pipeline.SignalLogs, ID: exporterID, Telemetry: telemetry.NewTelemetrySettings(),
	}, delegate)
	require.NoError(t, err)
	require.NoError(t, obs.Offer(context.Background(), &requesttest.FakeRequest{Items: 5}))
	delegate.offerErr = errors.New("queue full")
	require.ErrorIs(t, obs.Offer(context.Background(), &requesttest.FakeRequest{Items: 2}), delegate.offerErr)
	require.ErrorIs(t, obs.Offer(context.Background(), &requesttest.FakeRequest{Items: 3}), delegate.offerErr)
	metadatatest.AssertEqualExporterEnqueueFailedLogRecords(t, telemetry,
		[]metricdata.DataPoint[int64]{{Attributes: attribute.NewSet(attribute.String(exporterKey, exporterID.String())), Value: 5}},
		metricdatatest.IgnoreTimestamp(), metricdatatest.IgnoreExemplars())
}

func TestObsQueueLogsSuccessAfterFailurePreservesCount(t *testing.T) {
	telemetry := componenttest.NewTelemetry()
	t.Cleanup(func() { require.NoError(t, telemetry.Shutdown(context.Background())) })
	delegate := &fakeQueue[request.Request]{capacity: 10, offerErr: errors.New("queue full")}
	obs, err := newObsQueue[request.Request](Settings[request.Request]{
		Signal: pipeline.SignalLogs, ID: exporterID, Telemetry: telemetry.NewTelemetrySettings(),
	}, delegate)
	require.NoError(t, err)
	require.ErrorIs(t, obs.Offer(context.Background(), &requesttest.FakeRequest{Items: 2}), delegate.offerErr)
	delegate.offerErr = nil
	require.NoError(t, obs.Offer(context.Background(), &requesttest.FakeRequest{Items: 5}))
	metadatatest.AssertEqualExporterEnqueueFailedLogRecords(t, telemetry,
		[]metricdata.DataPoint[int64]{{Attributes: attribute.NewSet(attribute.String(exporterKey, exporterID.String())), Value: 2}},
		metricdatatest.IgnoreTimestamp(), metricdatatest.IgnoreExemplars())
}
