// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later
package db

import "testing"

func TestObservationSignalNullAndZeroPostgres(t *testing.T) {
	f := newPsFixture(t)
	for i := range 2 {
		o := psObs(f.psPacket(i), 1, "AAA", psT0)
		o.RSSI, o.SNR = nil, nil
		if i == 1 {
			r, s := int16(0), float32(0)
			o.RSSI, o.SNR = &r, &s
		}
		if _, _, err := f.store.InsertObservation(f.ctx, o); err != nil {
			t.Fatal(err)
		}
		var r *int16
		var s *float32
		if err := f.pool.QueryRow(f.ctx, "SELECT rssi,snr FROM packet_observations WHERE packet_hash=$1", o.PacketHash).Scan(&r, &s); err != nil {
			t.Fatal(err)
		}
		if i == 0 && (r != nil || s != nil) {
			t.Fatal("missing signal became a measurement")
		}
		if i == 1 && (r == nil || s == nil || *r != 0 || *s != 0) {
			t.Fatal("measured zero was lost")
		}
	}
}
