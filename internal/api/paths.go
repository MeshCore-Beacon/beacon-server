// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package api

// PathStats counts stored receptions in [Since, Until), not unique packets or nodes.
// Hashed + Empty + Trace + Unclassified partitions Receptions. Empty paths never
// vote for a hash width. TRACE header paths contain signal readings, not hashes.
// Since and Until are the effective UTC-hour boundaries of the materialized snapshot.
type PathStats struct {
	Since        int64           `json:"since"`
	Until        int64           `json:"until"`
	Receptions   int64           `json:"receptions"`
	Hashed       int64           `json:"hashed"`
	Empty        int64           `json:"empty"`
	Trace        int64           `json:"trace"`
	Unclassified int64           `json:"unclassified"`
	HashWidths   []PathHashWidth `json:"hashWidths"`
	PathLengths  []PathLengthBin `json:"pathLengths"`
	Hourly       []PathHour      `json:"hourly"`
}

type PathHashWidth struct {
	Bytes      int32 `json:"bytes"`
	Receptions int64 `json:"receptions"`
}

// PathLengthBin counts entries in validated ordinary header paths, including empty.
// Flood paths accumulate entries; direct paths contain remaining route entries.
// These counts cannot establish distance or a complete end-to-end hop count.
type PathLengthBin struct {
	Entries    int32 `json:"entries"`
	Receptions int64 `json:"receptions"`
}

// PathHour is a UTC hour clipped to the requested window. Missing hours are omitted.
type PathHour struct {
	Hour         int64 `json:"hour"`
	Receptions   int64 `json:"receptions"`
	OneByte      int64 `json:"oneByte"`
	TwoByte      int64 `json:"twoByte"`
	ThreeByte    int64 `json:"threeByte"`
	Empty        int64 `json:"empty"`
	Trace        int64 `json:"trace"`
	Unclassified int64 `json:"unclassified"`
}
