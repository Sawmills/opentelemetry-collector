// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package otelcol

import (
	"context"
	"errors"
	"net"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/extension"
)

func TestCollectorStartupTimeoutCancelsComponentStart(t *testing.T) {
	set := startupTimeoutSettings(t, func(ctx context.Context, _ component.Host) error {
		<-ctx.Done()
		return ctx.Err()
	}, nil)
	set.StartupTimeout = 50 * time.Millisecond
	col, err := NewCollector(set)
	require.NoError(t, err)
	parent, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	require.ErrorIs(t, col.Run(parent), context.DeadlineExceeded)
	require.NoError(t, parent.Err(), "startup must expire before the caller context")
	require.Equal(t, StateClosed, col.GetState())
}

func TestCollectorStartupTimeoutCancelsConfigRetrieval(t *testing.T) {
	provider := newFakeProvider("wait", func(ctx context.Context, _ string, _ confmap.WatcherFunc) (*confmap.Retrieved, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	})
	col, err := NewCollector(CollectorSettings{
		Factories:      nopFactories,
		StartupTimeout: 50 * time.Millisecond,
		ConfigProviderSettings: ConfigProviderSettings{ResolverSettings: confmap.ResolverSettings{
			URIs: []string{"wait:config"}, ProviderFactories: []confmap.ProviderFactory{provider},
		}},
	})
	require.NoError(t, err)
	parent, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	require.ErrorIs(t, col.Run(parent), context.DeadlineExceeded)
	require.NoError(t, parent.Err(), "configuration retrieval must share the startup deadline")
	require.Equal(t, StateClosed, col.GetState())
}

func TestCollectorStartupTimeoutNegativeIsExpiredAtConfigurationEntry(t *testing.T) {
	previous := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(previous)
	var enteredWithLiveContext atomic.Bool
	provider := newFakeProvider("owned", func(ctx context.Context, _ string, _ confmap.WatcherFunc) (*confmap.Retrieved, error) {
		enteredWithLiveContext.Store(ctx.Err() == nil)
		return nil, errors.New("owned configuration read ended")
	})
	col, err := NewCollector(CollectorSettings{
		Factories:      nopFactories,
		StartupTimeout: -time.Second,
		ConfigProviderSettings: ConfigProviderSettings{ResolverSettings: confmap.ResolverSettings{
			URIs: []string{"owned:config"}, ProviderFactories: []confmap.ProviderFactory{provider},
		}},
	})
	require.NoError(t, err)
	require.ErrorIs(t, col.Run(t.Context()), context.DeadlineExceeded)
	require.False(t, enteredWithLiveContext.Load(), "negative startup timeout reached configuration retrieval with a live context")
}

func TestCollectorStartupTimeoutPreservesHealthyLifetime(t *testing.T) {
	started := make(chan context.Context, 1)
	set := startupTimeoutSettings(t, func(ctx context.Context, _ component.Host) error {
		started <- ctx
		return nil
	}, nil)
	set.StartupTimeout = time.Minute
	col, err := NewCollector(set)
	require.NoError(t, err)
	done := make(chan error, 1)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go func() { done <- col.Run(ctx) }()
	t.Cleanup(col.Shutdown)
	require.Eventually(t, func() bool { return col.GetState() == StateRunning }, time.Second, time.Millisecond)
	componentContext := <-started
	select {
	case <-componentContext.Done():
		t.Fatal("successful startup cancelled the context retained by a running component")
	default:
	}
	col.Shutdown()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("collector did not stop after Shutdown")
	}
	require.Equal(t, StateClosed, col.GetState())
	select {
	case <-componentContext.Done():
	case <-time.After(time.Second):
		t.Fatal("completed collector retained its component lifetime context")
	}
}

func TestCollectorStartupTimeoutPreservesStartupFailure(t *testing.T) {
	want := errors.New("component startup failed")
	set := startupTimeoutSettings(t, func(context.Context, component.Host) error { return want }, nil)
	set.StartupTimeout = time.Second
	col, err := NewCollector(set)
	require.NoError(t, err)
	require.ErrorIs(t, col.Run(t.Context()), want)
	require.Equal(t, StateClosed, col.GetState())
}

func TestCollectorStartupTimeoutRejectsLateSuccess(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()
	var cleanupEntryError error
	set := startupTimeoutSettings(t, func(ctx context.Context, _ component.Host) error {
		<-ctx.Done()
		return nil
	}, func(ctx context.Context) error {
		cleanupEntryError = ctx.Err()
		if cleanupEntryError != nil {
			return cleanupEntryError
		}
		return listener.Close()
	})
	set.StartupTimeout = 50 * time.Millisecond
	col, err := NewCollector(set)
	require.NoError(t, err)
	parent, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	require.ErrorIs(t, col.Run(parent), context.DeadlineExceeded,
		"a successful Start cannot erase the configured startup deadline")
	require.NoError(t, parent.Err())
	require.Equal(t, StateClosed, col.GetState())
	require.NoError(t, cleanupEntryError, "startup cancellation must leave cleanup able to release resources")
	peer, err := net.DialTimeout("tcp", listener.Addr().String(), time.Second)
	if peer != nil {
		_ = peer.Close()
	}
	require.Error(t, err, "Run must release started resources before reporting the startup failure")
}

func TestCollectorStartupTimeoutDoesNotReturnBeforeStart(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	releaseStart := sync.OnceFunc(func() { close(release) })
	set := startupTimeoutSettings(t, func(ctx context.Context, _ component.Host) error {
		close(entered)
		<-release
		return ctx.Err()
	}, nil)
	set.StartupTimeout = 50 * time.Millisecond
	col, err := NewCollector(set)
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() { done <- col.Run(t.Context()) }()
	defer releaseStart()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("component Start was not called")
	}
	select {
	case err := <-done:
		t.Fatalf("Run reported completion while Start still owned work: %v", err)
	case <-time.After(2 * set.StartupTimeout):
	}
	require.Equal(t, StateStarting, col.GetState(), "context-ignoring Start remains unqualified")
	releaseStart()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.DeadlineExceeded)
	case <-time.After(time.Second):
		t.Fatal("collector did not finish after Start returned")
	}
}

func TestCollectorStartupTimeoutZeroPreservesCallerContext(t *testing.T) {
	parent, cancel := context.WithCancel(t.Context())
	defer cancel()
	entered := make(chan context.Context, 1)
	set := startupTimeoutSettings(t, func(ctx context.Context, _ component.Host) error {
		entered <- ctx
		<-ctx.Done()
		return ctx.Err()
	}, nil)
	set.StartupTimeout = 0
	col, err := NewCollector(set)
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() { done <- col.Run(parent) }()
	select {
	case ctx := <-entered:
		require.Same(t, parent, ctx)
	case <-time.After(time.Second):
		t.Fatal("component Start was not called")
	}
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("collector ignored caller cancellation")
	}
}

// Register a lifecycle adapter through CollectorSettings, the same public seam
// available to distributions. No service or collector internals are replaced.
func startupTimeoutSettings(t *testing.T, start component.StartFunc, shutdown component.ShutdownFunc) CollectorSettings {
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
					return &startupTimeoutExtension{StartFunc: start, ShutdownFunc: shutdown}, nil
				}, component.StabilityLevelStable)
			factories.Extensions[factory.Type()] = factory
			return factories, nil
		},
	}
}

type startupTimeoutExtension struct {
	component.StartFunc
	component.ShutdownFunc
}
