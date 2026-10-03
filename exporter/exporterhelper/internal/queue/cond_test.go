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

func TestCondSignalWakesOneWaiter(t *testing.T) {
	var mu sync.Mutex
	c := newCond(&mu)

	errCh := make(chan error, 2)
	for range 2 {
		go func() {
			mu.Lock()
			defer mu.Unlock()
			errCh <- c.Wait(context.Background())
		}()
	}

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(c.waiters) == 2
	}, time.Second, 10*time.Millisecond)

	mu.Lock()
	c.Signal()
	mu.Unlock()

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("Signal did not wake a waiter")
	}

	select {
	case err := <-errCh:
		require.NoError(t, err)
		t.Fatal("Signal woke more than one waiter")
	case <-time.After(50 * time.Millisecond):
	}

	mu.Lock()
	c.Signal()
	mu.Unlock()

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("second waiter was not released")
	}
}

func TestCondCanceledSignaledWaiterResignalsNextWaiter(t *testing.T) {
	var mu sync.Mutex
	relocking := make(chan struct{}, 2)
	c := newCond(&relockNotifier{locker: &mu, relocking: relocking})
	t.Cleanup(func() {
		mu.Lock()
		c.Broadcast()
		mu.Unlock()
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	firstErrCh := make(chan error, 1)
	secondErrCh := make(chan error, 1)

	go func() {
		mu.Lock()
		defer mu.Unlock()
		firstErrCh <- c.Wait(ctx)
	}()
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(c.waiters) == 1
	}, time.Second, time.Millisecond)
	go func() {
		mu.Lock()
		defer mu.Unlock()
		secondErrCh <- c.Wait(context.Background())
	}()

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(c.waiters) == 2
	}, time.Second, 10*time.Millisecond)

	mu.Lock()
	cancel()
	// Wait until the first waiter has selected cancellation and is trying to
	// reacquire the mutex. Signaling it now must forward the wake to waiter two.
	select {
	case <-relocking:
	case <-time.After(time.Second):
		mu.Unlock()
		t.Fatal("canceled waiter did not try to reacquire the mutex")
	}
	c.Signal()
	mu.Unlock()

	select {
	case err := <-firstErrCh:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("canceled waiter did not return")
	}

	select {
	case err := <-secondErrCh:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("signal was not forwarded to the next waiter")
	}
}

type relockNotifier struct {
	locker    sync.Locker
	relocking chan<- struct{}
}

func (l *relockNotifier) Lock() {
	l.relocking <- struct{}{}
	l.locker.Lock()
}

func (l *relockNotifier) Unlock() {
	l.locker.Unlock()
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
