// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package guards

import "time"

// Class is the cost shape of a request, which sets its database deadline.
type Class int

// The request classes.
const (
	Entity Class = iota
	List
	Aggregation
)

// Deadline is the database time a request of this class may use, counted from
// its first statement.
func (c Class) Deadline() time.Duration {
	switch c {
	case List:
		return 10 * time.Second
	case Aggregation:
		return 30 * time.Second
	default:
		return 5 * time.Second
	}
}

func (c Class) String() string {
	switch c {
	case List:
		return "list"
	case Aggregation:
		return "aggregation"
	default:
		return "entity"
	}
}
