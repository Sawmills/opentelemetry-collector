// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

//go:build unix

package otelcol

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"go.opentelemetry.io/collector/component"
)

func TestCollectorShutdownTimeoutSignals(t *testing.T) {
	const childMode = "OTELCOL_SHUTDOWN_TIMEOUT_TEST_CHILD"
	const ready = "collector-shutdown-timeout-ready"
	if mode := os.Getenv(childMode); mode != "" {
		set := shutdownTimeoutSettings(t, func(context.Context, component.Host) error {
			_, err := fmt.Fprintln(os.Stdout, ready)
			return err
		}, nil)
		set.DisableGracefulShutdown = strings.HasSuffix(mode, "disabled")
		var args []string
		if strings.HasPrefix(mode, "zero") {
			args = []string{"--shutdown-timeout=0"}
		} else if strings.HasPrefix(mode, "configured") {
			args = []string{"--shutdown-timeout=1s"}
		}
		cmd := NewCommand(set)
		cmd.SetArgs(args)
		require.NoError(t, cmd.ExecuteContext(context.Background()))
		return
	}
	for _, mode := range []string{"absent-normal", "zero-normal", "configured-normal", "absent-disabled", "zero-disabled", "configured-disabled"} {
		t.Run(mode, func(t *testing.T) {
			//nolint:gosec // #nosec G204 -- re-executes this test binary with fixed arguments.
			cmd := exec.Command(os.Args[0], "-test.run=^TestCollectorShutdownTimeoutSignals$", "-test.timeout=10s")
			cmd.Env = append(os.Environ(), childMode+"="+mode)
			stdout, err := cmd.StdoutPipe()
			require.NoError(t, err)
			var stderr bytes.Buffer
			cmd.Stderr = &stderr
			require.NoError(t, cmd.Start())
			exited := make(chan struct{})
			var exitErr error
			go func() { exitErr = cmd.Wait(); close(exited) }()
			t.Cleanup(func() {
				select {
				case <-exited:
				default:
					_ = cmd.Process.Kill()
					<-exited
				}
			})
			started := make(chan string, 1)
			go func() {
				line, _ := bufio.NewReader(stdout).ReadString('\n')
				started <- line
			}()
			select {
			case line := <-started:
				require.Equal(t, ready+"\n", line)
			case <-exited:
				t.Fatalf("collector exited before startup: %v\n%s", exitErr, stderr.String())
			case <-time.After(5 * time.Second):
				t.Fatal("collector did not enter Start")
			}
			require.NoError(t, cmd.Process.Signal(syscall.SIGTERM))
			select {
			case <-exited:
			case <-time.After(5 * time.Second):
				t.Fatal("collector did not exit after SIGTERM")
			}
			if strings.HasSuffix(mode, "disabled") {
				var status *exec.ExitError
				require.ErrorAs(t, exitErr, &status)
				wait, ok := status.Sys().(syscall.WaitStatus)
				require.True(t, ok)
				require.True(t, wait.Signaled())
				require.Equal(t, syscall.SIGTERM, wait.Signal())
			} else {
				require.NoError(t, exitErr, stderr.String())
			}
		})
	}
}
