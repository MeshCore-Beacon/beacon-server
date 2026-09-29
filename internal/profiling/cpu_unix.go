// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

//go:build linux || darwin

package profiling

import "syscall"

func cpuUsage() *processCPU {
	var usage syscall.Rusage
	if syscall.Getrusage(syscall.RUSAGE_SELF, &usage) != nil {
		return nil
	}
	return &processCPU{
		UserSeconds:   float64(usage.Utime.Sec) + float64(usage.Utime.Usec)/1e6,
		SystemSeconds: float64(usage.Stime.Sec) + float64(usage.Stime.Usec)/1e6,
	}
}
