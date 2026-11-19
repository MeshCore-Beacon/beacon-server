// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

// storedSignal returns an observation's RSSI and SNR as readers show them. Rows written before missing readings
// were stored as NULL hold 0 for both, which no receiver reports, so that pair reads as not measured.
func storedSignal(rssi *int16, snr *float32) (*int16, *float32) {
	if rssi != nil && snr != nil && *rssi == 0 && *snr == 0 {
		return nil, nil
	}
	return rssi, snr
}
