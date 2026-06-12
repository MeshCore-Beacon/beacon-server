// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package background runs periodic maintenance tasks on independent schedules.
package background

import (
	"context"
	"log"
	"time"
)

// Task is a named unit of work that runs on a fixed interval.
type Task struct {
	Name     string
	Interval time.Duration
	Run      func(ctx context.Context) error
}

// Scheduler runs a set of tasks on independent tickers.
type Scheduler struct {
	tasks []Task
}

// New creates a Scheduler with the given tasks.
func New(tasks []Task) *Scheduler {
	return &Scheduler{tasks: tasks}
}

// Start launches each task in its own goroutine. Blocks until ctx is cancelled.
func (s *Scheduler) Start(ctx context.Context) {
	for _, t := range s.tasks {
		go func() {
			ticker := time.NewTicker(t.Interval)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					log.Printf("background[%s]: running", t.Name)
					if err := t.Run(ctx); err != nil {
						log.Printf("background[%s]: %v", t.Name, err)
					}
					log.Printf("background[%s]: complete", t.Name)
				case <-ctx.Done():
					return
				}
			}
		}()
	}
}
