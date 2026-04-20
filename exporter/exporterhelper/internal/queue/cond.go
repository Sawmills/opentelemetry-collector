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
	L       sync.Locker
	waiters []*condWaiter
}

func newCond(l sync.Locker) *cond {
	return &cond{L: l}
}

type condWaiter struct {
	ch chan struct{}
}

// Signal wakes one goroutine waiting on c, if there is any.
// It requires for the caller to hold c.L during the call.
func (c *cond) Signal() {
	if len(c.waiters) == 0 {
		return
	}

	waiter := c.waiters[0]
	c.waiters = c.waiters[1:]
	close(waiter.ch)
}

// Broadcast wakes all goroutines waiting on c.
// It requires for the caller to hold c.L during the call.
func (c *cond) Broadcast() {
	for _, waiter := range c.waiters {
		close(waiter.ch)
	}
	c.waiters = nil
}

// Wait atomically unlocks c.L and suspends execution of the calling goroutine. After later resuming execution, Wait locks c.L before returning.
func (c *cond) Wait(ctx context.Context) error {
	waiter := &condWaiter{ch: make(chan struct{})}
	c.waiters = append(c.waiters, waiter)
	c.L.Unlock()
	select {
	case <-ctx.Done():
		c.L.Lock()
		removed := false
		for i, w := range c.waiters {
			if w == waiter {
				copy(c.waiters[i:], c.waiters[i+1:])
				c.waiters = c.waiters[:len(c.waiters)-1]
				removed = true
				break
			}
		}
		if !removed {
			c.Signal()
		}
		return ctx.Err()
	case <-waiter.ch:
		c.L.Lock()
		return nil
	}
}
