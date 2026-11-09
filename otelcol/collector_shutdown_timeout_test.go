// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package otelcol

import (
	"context"
	"errors"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/extension"
)

func TestCollectorShutdownTimeoutCancelsCleanupAndReleasesResources(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()
	set := shutdownTimeoutSettings(t, nil, func(ctx context.Context) error {
		<-ctx.Done()
		_ = listener.Close()
		return ctx.Err()
	})
	set.ShutdownTimeout = 50 * time.Millisecond
	parent, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	col, done := runShutdownTimeoutCollector(parent, t, set)
	col.Shutdown()
	require.ErrorIs(t, waitShutdownTimeoutCollector(t, done), context.DeadlineExceeded)
	require.NoError(t, parent.Err(), "configured shutdown must expire before the running caller")
	peer, err := net.DialTimeout("tcp", listener.Addr().String(), time.Second)
	if peer != nil {
		_ = peer.Close()
	}
	require.Error(t, err, "Run must release owned resources before reporting cleanup failure")
	require.Equal(t, StateClosed, col.GetState())
}

func TestCollectorShutdownTimeoutRejectsLateSuccess(t *testing.T) {
	set := shutdownTimeoutSettings(t, nil, func(ctx context.Context) error { <-ctx.Done(); return nil })
	set.ShutdownTimeout = 50 * time.Millisecond
	parent, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	col, done := runShutdownTimeoutCollector(parent, t, set)
	col.Shutdown()
	require.ErrorIs(t, waitShutdownTimeoutCollector(t, done), context.DeadlineExceeded)
	require.NoError(t, parent.Err())
}

func TestCollectorShutdownTimeoutSharesConfigurationAndServiceBudget(t *testing.T) {
	var configurationDeadline, serviceDeadline time.Time
	var serviceExpired bool
	set := shutdownTimeoutSettings(t, nil, func(ctx context.Context) error {
		serviceDeadline, _ = ctx.Deadline()
		serviceExpired = errors.Is(ctx.Err(), context.DeadlineExceeded)
		return ctx.Err()
	})
	set.ShutdownTimeout = 50 * time.Millisecond
	provider := newFakeProvider("shared", func(context.Context, string, confmap.WatcherFunc) (*confmap.Retrieved, error) {
		return confmap.NewRetrieved(newConfFromFile(t, filepath.Join("testdata", "otelcol-nop.yaml")),
			confmap.WithRetrievedClose(func(ctx context.Context) error {
				var hasDeadline bool
				configurationDeadline, hasDeadline = ctx.Deadline()
				if !hasDeadline {
					return errors.New("configuration cleanup has no deadline")
				}
				<-ctx.Done()
				return ctx.Err()
			}))
	})
	set.ConfigProviderSettings = ConfigProviderSettings{ResolverSettings: confmap.ResolverSettings{
		URIs: []string{"shared:config"}, ProviderFactories: []confmap.ProviderFactory{provider},
	}}
	col, done := runShutdownTimeoutCollector(t.Context(), t, set)
	col.Shutdown()
	require.ErrorIs(t, waitShutdownTimeoutCollector(t, done), context.DeadlineExceeded)
	require.False(t, configurationDeadline.IsZero())
	require.Equal(t, configurationDeadline, serviceDeadline, "both cleanup stages must share the original deadline")
	require.True(t, serviceExpired, "configuration cleanup must consume the shared budget before service cleanup")
}

func TestCollectorShutdownTimeoutZeroPreservesCallerContext(t *testing.T) {
	parent, cancel := context.WithCancel(t.Context())
	defer cancel()
	seen := make(chan context.Context, 1)
	set := shutdownTimeoutSettings(t, nil, func(ctx context.Context) error { seen <- ctx; return nil })
	set.ShutdownTimeout = 0
	col, done := runShutdownTimeoutCollector(parent, t, set)
	col.Shutdown()
	require.NoError(t, waitShutdownTimeoutCollector(t, done))
	require.Same(t, parent, <-seen)
}

func TestCollectorShutdownTimeoutAbsentPreservesIndependentScopeAfterCallerCancellation(t *testing.T) {
	parent, cancel := context.WithCancel(t.Context())
	defer cancel()
	seen := make(chan context.Context, 1)
	set := shutdownTimeoutSettings(t, nil, func(ctx context.Context) error { seen <- ctx; return nil })
	_, done := runShutdownTimeoutCollector(parent, t, set)
	cancel()
	require.NoError(t, waitShutdownTimeoutCollector(t, done))
	ctx := <-seen
	require.NoError(t, ctx.Err())
	_, hasDeadline := ctx.Deadline()
	require.False(t, hasDeadline)
}

func TestCollectorShutdownTimeoutRetainsShorterCallerDeadline(t *testing.T) {
	var deadline time.Time
	var hasDeadline bool
	set := shutdownTimeoutSettings(t, nil, func(ctx context.Context) error {
		deadline, hasDeadline = ctx.Deadline()
		return nil
	})
	set.ShutdownTimeout = 2 * time.Minute
	parent, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	col, done := runShutdownTimeoutCollector(parent, t, set)
	col.Shutdown()
	require.NoError(t, waitShutdownTimeoutCollector(t, done))
	want, _ := parent.Deadline()
	require.True(t, hasDeadline)
	require.Equal(t, want, deadline, "the shorter caller deadline must remain the cleanup cap")
	require.NoError(t, parent.Err())
}

func TestCollectorShutdownTimeoutConfiguredAfterCallerCancellation(t *testing.T) {
	parent, cancel := context.WithCancel(t.Context())
	defer cancel()
	var entryError error
	var deadline, enteredAt time.Time
	var hasDeadline bool
	set := shutdownTimeoutSettings(t, nil, func(ctx context.Context) error {
		entryError = ctx.Err()
		deadline, hasDeadline = ctx.Deadline()
		enteredAt = time.Now()
		return nil
	})
	set.ShutdownTimeout = time.Minute
	_, done := runShutdownTimeoutCollector(parent, t, set)
	cancel()
	require.NoError(t, waitShutdownTimeoutCollector(t, done))
	require.NoError(t, entryError, "cleanup must start independently of the canceled running caller")
	require.True(t, hasDeadline)
	require.Positive(t, deadline.Sub(enteredAt))
	require.LessOrEqual(t, deadline.Sub(enteredAt), set.ShutdownTimeout)
	require.ErrorIs(t, parent.Err(), context.Canceled)
}

func TestCollectorShutdownTimeoutNegativeIsExpiredAtCleanupEntry(t *testing.T) {
	var entryError error
	set := shutdownTimeoutSettings(t, nil, func(ctx context.Context) error {
		entryError = ctx.Err()
		return entryError
	})
	set.ShutdownTimeout = -time.Second
	col, done := runShutdownTimeoutCollector(t.Context(), t, set)
	col.Shutdown()
	require.ErrorIs(t, waitShutdownTimeoutCollector(t, done), context.DeadlineExceeded)
	require.ErrorIs(t, entryError, context.DeadlineExceeded)
}

func TestCollectorShutdownTimeoutNonZeroCommandFlag(t *testing.T) {
	parent, cancel := context.WithCancel(t.Context())
	defer cancel()
	started, release := make(chan struct{}), make(chan struct{})
	releaseCleanup := sync.OnceFunc(func() { close(release) })
	t.Cleanup(releaseCleanup)
	set := shutdownTimeoutSettings(t, func(context.Context, component.Host) error {
		close(started)
		return nil
	}, func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-release:
			return nil
		}
	})
	cmd := NewCommand(set)
	cmd.SetArgs([]string{"--shutdown-timeout=100ms"})
	done := make(chan error, 1)
	go func() { done <- cmd.ExecuteContext(parent) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("command did not start the component")
	}
	cancel()
	require.ErrorIs(t, waitShutdownTimeoutCollector(t, done), context.DeadlineExceeded)
}

func TestCollectorShutdownTimeoutPreservesHealthyAndFailedCleanup(t *testing.T) {
	want := errors.New("component cleanup failed")
	for _, cleanupError := range []error{nil, want} {
		set := shutdownTimeoutSettings(t, nil, func(context.Context) error { return cleanupError })
		set.ShutdownTimeout = time.Second
		col, done := runShutdownTimeoutCollector(t.Context(), t, set)
		col.Shutdown()
		require.ErrorIs(t, waitShutdownTimeoutCollector(t, done), cleanupError)
	}
}

func TestCollectorShutdownTimeoutDoesNotReturnBeforeCleanup(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	releaseCleanup := sync.OnceFunc(func() { close(release) })
	defer releaseCleanup()
	set := shutdownTimeoutSettings(t, nil, func(context.Context) error {
		close(entered)
		<-release
		return nil
	})
	set.ShutdownTimeout = 50 * time.Millisecond
	col, done := runShutdownTimeoutCollector(t.Context(), t, set)
	col.Shutdown()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("cleanup was not called")
	}
	select {
	case err := <-done:
		t.Fatalf("Run returned while cleanup still owned work: %v", err)
	case <-time.After(2 * set.ShutdownTimeout):
	}
	require.Equal(t, StateClosing, col.GetState(), "context-ignoring cleanup remains unqualified")
	releaseCleanup()
	require.ErrorIs(t, waitShutdownTimeoutCollector(t, done), context.DeadlineExceeded)
}

func runShutdownTimeoutCollector(ctx context.Context, t *testing.T, set CollectorSettings) (*Collector, <-chan error) {
	t.Helper()
	col, err := NewCollector(set)
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() { done <- col.Run(ctx) }()
	t.Cleanup(col.Shutdown)
	require.Eventually(t, func() bool { return col.GetState() == StateRunning }, time.Second, time.Millisecond)
	return col, done
}

func waitShutdownTimeoutCollector(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(4 * time.Second):
		t.Fatal("collector cleanup did not return")
		return nil
	}
}

func shutdownTimeoutSettings(t *testing.T, start component.StartFunc, stop component.ShutdownFunc) CollectorSettings {
	t.Helper()
	return CollectorSettings{
		BuildInfo:              component.NewDefaultBuildInfo(),
		ConfigProviderSettings: newDefaultConfigProviderSettings(t, []string{filepath.Join("testdata", "otelcol-nop.yaml")}),
		Factories: func() (Factories, error) {
			factories, err := nopFactories()
			if err != nil {
				return factories, err
			}
			factory := extension.NewFactory(component.MustNewType("nop"),
				func() component.Config { return &struct{}{} },
				func(context.Context, extension.Settings, component.Config) (extension.Extension, error) {
					return &shutdownTimeoutExtension{StartFunc: start, ShutdownFunc: stop}, nil
				}, component.StabilityLevelStable)
			factories.Extensions[factory.Type()] = factory
			return factories, nil
		},
	}
}

type shutdownTimeoutExtension struct {
	component.StartFunc
	component.ShutdownFunc
}
