// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package queue // import "go.opentelemetry.io/collector/exporter/exporterhelper/internal/queue"

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCondSignalDoesNotBlockWithPendingWakeup(t *testing.T) {
	var mu sync.Mutex
	c := newCond(&mu)

	mu.Lock()
	c.Signal()
	done := make(chan struct{})
	go func() {
		mu.Lock()
		defer mu.Unlock()
		c.Signal()
		close(done)
	}()
	mu.Unlock()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Signal blocked with a pending wakeup")
	}
}

func TestCondWaitReturnsOnContextCancel(t *testing.T) {
	var mu sync.Mutex
	c := newCond(&mu)
	ctx, cancel := context.WithCancel(context.Background())

	errCh := make(chan error, 1)
	go func() {
		mu.Lock()
		defer mu.Unlock()
		errCh <- c.Wait(ctx)
	}()

	time.Sleep(10 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("Wait did not return after context cancellation")
	}
}
