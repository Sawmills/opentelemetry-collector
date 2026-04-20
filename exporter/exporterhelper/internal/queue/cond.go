// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package queue // import "go.opentelemetry.io/collector/exporter/exporterhelper/internal/queue"

import (
	"context"
	"sync"
)

// cond is equivalent with sync.Cond, but context.Context aware.
// Which means Wait() will return if context is done before any signal is received.
// Also, it requires the caller to hold the c.L during all calls.
type cond struct {
	L  sync.Locker
	ch chan struct{}
}

func newCond(l sync.Locker) *cond {
	return &cond{L: l, ch: make(chan struct{}, 1)}
}

// Signal wakes goroutines waiting on c, if there are any.
// It requires for the caller to hold c.L during the call.
func (c *cond) Signal() {
	close(c.ch)
	c.ch = make(chan struct{}, 1)
}

// Broadcast wakes all goroutines waiting on c.
// It requires for the caller to hold c.L during the call.
func (c *cond) Broadcast() {
	c.Signal()
}

// Wait atomically unlocks c.L and suspends execution of the calling goroutine. After later resuming execution, Wait locks c.L before returning.
func (c *cond) Wait(ctx context.Context) error {
	ch := c.ch
	c.L.Unlock()
	select {
	case <-ctx.Done():
		c.L.Lock()
		return ctx.Err()
	case <-ch:
		c.L.Lock()
		return nil
	}
}
