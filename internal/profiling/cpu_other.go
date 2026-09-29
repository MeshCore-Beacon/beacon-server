// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

//go:build !linux && !darwin

package profiling

func cpuUsage() *processCPU { return nil }
