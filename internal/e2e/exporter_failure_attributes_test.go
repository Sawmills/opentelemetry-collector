// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package e2e

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/confmap/provider/envprovider"
	"go.opentelemetry.io/collector/confmap/provider/fileprovider"
	"go.opentelemetry.io/collector/confmap/provider/yamlprovider"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/otlphttpexporter"
	"go.opentelemetry.io/collector/otelcol"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/otlpreceiver"
	"go.opentelemetry.io/collector/service/telemetry/otelconftelemetry"
)

func TestExporterFailureAttributesDetailed(t *testing.T) {
	t.Run("permanent error sets error.permanent", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/v1/metrics" {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			http.Error(w, "bad request", http.StatusBadRequest)
		}))
		defer server.Close()

		otelPort, metricsPort := startFailureAttributeCollector(t, server.URL)

		require.NoError(t, sendTestMetrics(otelPort))

		require.Eventually(t, func() bool {
			metric := scrapeFailureMetric(t, metricsPort, "otlp_http/test")
			if metric == nil {
				return false
			}
			failurePermanent, ok := labelValue(metric, "error_permanent")
			return ok && failurePermanent == "true"
		}, 5*time.Second, 200*time.Millisecond, "expected permanent failure metric")
	})

	t.Run("transient error that recovers records zero failed items", func(t *testing.T) {
		var attempts atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/v1/metrics" {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			if attempts.Add(1) == 1 {
				http.Error(w, "try again", http.StatusServiceUnavailable)
				return
			}
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		otelPort, metricsPort := startFailureAttributeCollector(t, server.URL)

		require.NoError(t, sendTestMetrics(otelPort))
		require.Eventually(t, func() bool {
			return len(scrapeFailureMetrics(t, metricsPort, "otlp_http/test")) > 0
		}, 5*time.Second, 200*time.Millisecond, "expected observed failure-counter coverage after recovery")
		require.Equal(t, int32(2), attempts.Load(), "the transient failure must be retried successfully")
		metrics := scrapeFailureMetrics(t, metricsPort, "otlp_http/test")
		require.Len(t, metrics, 1, "recovery must not record a separate positive failure series")
		require.NotNil(t, metrics[0].Counter)
		require.Zero(t, metrics[0].Counter.GetValue())
		for _, label := range metrics[0].Label {
			require.False(t, strings.HasPrefix(label.GetName(), "error_"), "successful outcomes must not have error attributes")
		}
	})

	t.Run("retryable error exhausts retries", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/v1/metrics" {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			http.Error(w, "temporarily unavailable", http.StatusServiceUnavailable)
		}))
		defer server.Close()

		otelPort, metricsPort := startFailureAttributeCollector(t, server.URL)

		require.NoError(t, sendTestMetrics(otelPort))

		require.Eventually(t, func() bool {
			metric := scrapeFailureMetric(t, metricsPort, "otlp_http/test")
			if metric == nil {
				return false
			}
			failurePermanent, ok := labelValue(metric, "error_permanent")
			return ok && failurePermanent == "false"
		}, 5*time.Second, 200*time.Millisecond, "expected retry exhaustion metric")
	})
}

func startFailureAttributeCollector(t *testing.T, exporterEndpoint string) (string, string) {
	t.Helper()
	otelPort := getFreePort(t)
	metricsPort := getFreePort(t)

	t.Setenv("METRICS_PORT", metricsPort)
	t.Setenv("OTEL_PORT", otelPort)
	t.Setenv("EXPORTER_ENDPOINT", exporterEndpoint)

	collector, err := otelcol.NewCollector(otelcol.CollectorSettings{
		BuildInfo: component.NewDefaultBuildInfo(),
		Factories: func() (otelcol.Factories, error) {
			return otelcol.Factories{
				Receivers: map[component.Type]receiver.Factory{otlpreceiver.NewFactory().Type(): otlpreceiver.NewFactory()},
				Exporters: map[component.Type]exporter.Factory{
					otlphttpexporter.NewFactory().Type(): otlphttpexporter.NewFactory(),
				},
				Telemetry: otelconftelemetry.NewFactory(),
			}, nil
		},
		ConfigProviderSettings: otelcol.ConfigProviderSettings{
			ResolverSettings: confmap.ResolverSettings{
				URIs: []string{filepath.Join("testdata", "exporter_failure_attributes_test.yaml")},
				ProviderFactories: []confmap.ProviderFactory{
					fileprovider.NewFactory(),
					yamlprovider.NewFactory(),
					envprovider.NewFactory(),
				},
			},
		},
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	go func() {
		if err := collector.Run(ctx); err != nil {
			t.Logf("Collector stopped with error: %v", err)
		}
	}()

	require.Eventually(t, func() bool {
		resp, err := http.Get(fmt.Sprintf("http://localhost:%s/metrics", metricsPort))
		if err != nil {
			return false
		}
		resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	}, 5*time.Second, 100*time.Millisecond, "collector failed to start")

	return otelPort, metricsPort
}

func scrapeFailureMetric(t *testing.T, metricsPort, exporterName string) *dto.Metric {
	t.Helper()
	metrics := scrapeFailureMetrics(t, metricsPort, exporterName)
	if len(metrics) == 0 {
		return nil
	}
	return metrics[0]
}

func scrapeFailureMetrics(t *testing.T, metricsPort, exporterName string) []*dto.Metric {
	t.Helper()
	resp, err := http.Get(fmt.Sprintf("http://localhost:%s/metrics", metricsPort))
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil
	}

	parser := expfmt.NewTextParser(model.UTF8Validation)
	parsed, err := parser.TextToMetricFamilies(resp.Body)
	if err != nil {
		return nil
	}

	metricFamily, ok := parsed["otelcol_exporter_send_failed_metric_points"]
	if !ok {
		metricFamily, ok = parsed["otelcol_exporter_send_failed_metric_points_total"]
	}
	if !ok {
		return nil
	}

	var metrics []*dto.Metric
	for _, metric := range metricFamily.Metric {
		if hasLabel(metric, "exporter", exporterName) {
			metrics = append(metrics, metric)
		}
	}

	return metrics
}

func hasLabel(metric *dto.Metric, name, expected string) bool {
	for _, label := range metric.Label {
		if label.GetName() == name && label.GetValue() == expected {
			return true
		}
	}
	return false
}

func labelValue(metric *dto.Metric, labelName string) (string, bool) {
	for _, label := range metric.Label {
		if label.GetName() == labelName {
			return label.GetValue(), true
		}
	}
	return "", false
}
