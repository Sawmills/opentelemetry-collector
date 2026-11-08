// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//go:build unix

package otelcol

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v3"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/service/telemetry/otelconftelemetry"
)

func TestCollectorStartupSignalCompatibility(t *testing.T) {
	const childMode = "OTELCOL_STARTUP_SIGNAL_TEST_CHILD"
	const ready = "collector-startup-signal-ready"
	if mode := os.Getenv(childMode); mode != "" {
		configPath := filepath.Join("testdata", "otelcol-nop.yaml")
		factories := nopFactories
		if mode == "startup-held" {
			factories = func() (Factories, error) {
				configured, err := nopFactories()
				if err != nil {
					return configured, err
				}
				configured.Telemetry = otelconftelemetry.NewFactory()
				return configured, nil
			}
			data, err := os.ReadFile(filepath.Clean(configPath))
			require.NoError(t, err)
			var config map[string]any
			require.NoError(t, yaml.Unmarshal(data, &config))
			service := config["service"].(map[string]any)
			service["telemetry"] = map[string]any{"metrics": map[string]any{"level": "none"}, "logs": map[string]any{
				"processors": []any{map[string]any{"simple": map[string]any{
					"exporter": map[string]any{"otlp": map[string]any{
						"protocol": "http/protobuf", "endpoint": os.Getenv("OTELCOL_STARTUP_LOG_TEST_ENDPOINT"),
						"insecure": true, "timeout": 10000,
					}},
				}}},
			}}
			data, err = yaml.Marshal(config)
			require.NoError(t, err)
			configPath = filepath.Join(t.TempDir(), "collector.yaml")
			require.NoError(t, os.WriteFile(configPath, data, 0o600))
		}
		col, err := NewCollector(CollectorSettings{
			BuildInfo:               component.NewDefaultBuildInfo(),
			Factories:               factories,
			ConfigProviderSettings:  newDefaultConfigProviderSettings(t, []string{configPath}),
			DisableGracefulShutdown: mode == "disabled",
		})
		require.NoError(t, err)
		done := make(chan error, 1)
		go func() { done <- col.Run(context.Background()) }()
		if mode != "startup-held" {
			require.Eventually(t, func() bool { return col.GetState() == StateRunning }, 5*time.Second, time.Millisecond)
			_, err = fmt.Fprintln(os.Stdout, ready)
			require.NoError(t, err)
		}
		select {
		case err := <-done:
			require.NoError(t, err)
			require.Equal(t, StateClosed, col.GetState())
		case <-time.After(5 * time.Second):
			t.Fatal("collector did not stop after the parent sent SIGTERM")
		}
		return
	}

	for _, mode := range []string{"normal", "disabled", "startup-held"} {
		t.Run(mode, func(t *testing.T) {
			logHeld, logAck := make(chan struct{}), make(chan struct{})
			var heldOnce, ackOnce sync.Once
			release := func() { ackOnce.Do(func() { close(logAck) }) }
			var endpoint string
			if mode == "startup-held" {
				backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					body, err := io.ReadAll(r.Body)
					_ = r.Body.Close()
					if err != nil {
						w.WriteHeader(http.StatusBadRequest)
						return
					}
					if bytes.Contains(body, []byte("Everything is ready. Begin running and processing data.")) {
						heldOnce.Do(func() { close(logHeld) })
						select {
						case <-logAck:
						case <-r.Context().Done():
							return
						}
					}
					w.Header().Set("Content-Type", "application/x-protobuf")
					w.Header().Set("Connection", "close")
					w.WriteHeader(http.StatusOK)
				}))
				endpoint = backend.URL
				t.Cleanup(backend.Close)
				t.Cleanup(release)
			}
			//nolint:gosec // #nosec G204 -- re-executes this test binary with fixed arguments.
			command := exec.Command(os.Args[0], "-test.run=^TestCollectorStartupSignalCompatibility$", "-test.timeout=10s")
			command.Env = append(os.Environ(), childMode+"="+mode, "OTELCOL_STARTUP_LOG_TEST_ENDPOINT="+endpoint)
			stdout, err := command.StdoutPipe()
			require.NoError(t, err)
			var stderr bytes.Buffer
			command.Stderr = &stderr
			require.NoError(t, command.Start())
			exited := make(chan struct{})
			var exitErr error
			go func() {
				exitErr = command.Wait()
				close(exited)
			}()
			t.Cleanup(func() {
				select {
				case <-exited:
				default:
					_ = command.Process.Kill()
					<-exited
				}
			})
			started := make(chan string, 1)
			go func() {
				line, _ := bufio.NewReader(stdout).ReadString('\n')
				started <- line
			}()
			if mode == "startup-held" {
				select {
				case <-logHeld:
				case <-exited:
					t.Fatalf("collector exited before the startup log arrived: %v\n%s", exitErr, stderr.String())
				case <-time.After(5 * time.Second):
					t.Fatal("collector sent no startup log to the actual OTLP backend")
				}
			} else {
				select {
				case line := <-started:
					require.Equal(t, ready+"\n", line)
				case <-exited:
					t.Fatalf("collector exited before startup: %v\n%s", exitErr, stderr.String())
				case <-time.After(5 * time.Second):
					t.Fatal("collector did not report normal startup")
				}
			}
			require.NoError(t, command.Process.Signal(syscall.SIGTERM))
			if mode == "startup-held" {
				select {
				case <-exited:
					t.Fatalf("collector exited while startup acknowledgement was held: %v\n%s", exitErr, stderr.String())
				case <-time.After(100 * time.Millisecond):
				}
				release()
			}
			select {
			case <-exited:
			case <-time.After(5 * time.Second):
				t.Fatal("collector did not exit after SIGTERM")
			}
			if mode == "disabled" {
				var status *exec.ExitError
				require.ErrorAs(t, exitErr, &status)
				wait, ok := status.Sys().(syscall.WaitStatus)
				require.True(t, ok)
				require.True(t, wait.Signaled())
				require.Equal(t, syscall.SIGTERM, wait.Signal(), "disabled graceful shutdown retains the default signal action")
			} else {
				require.NoError(t, exitErr, stderr.String())
			}
		})
	}
}
