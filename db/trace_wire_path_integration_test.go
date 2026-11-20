// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"encoding/json"
	"testing"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
)

// A TRACE observation's pathBytes are the payload's hop hashes; wirePathBytes are the SNR bytes that were in the
// path slot on air, so header + pathLength.raw + wirePathBytes + payload is the frame that was heard.
func TestTraceWirePathBytesPostgres(t *testing.T) {
	ctx, pool := schemaPool(t)
	if err := RunMigrations(ctx, pool); err != nil {
		t.Fatal(err)
	}
	// On air: 26 02 28F4 11223344 00000000 00 A1B2C3, a three-hop trace that had reached two hops.
	if _, err := pool.Exec(ctx, `
 INSERT INTO iata_codes(iata) VALUES ('YVR');
 INSERT INTO observers(id,public_key) VALUES ('00000000-0000-0000-0000-000000000001','\x01'),
  ('00000000-0000-0000-0000-000000000002','\x02'),('00000000-0000-0000-0000-000000000003','\x03');
 INSERT INTO packets(packet_hash,payload_type,payload_version,route_type,raw_payload,raw_header,first_heard_at,last_heard_at) VALUES
  ('\x01',9,0,2,'\x112233440000000000a1b2c3','\x26',NOW(),NOW()),
  ('\x02',2,0,1,'\xaabbccddee','\x09',NOW(),NOW());
 INSERT INTO packet_observations(packet_hash,observer_id,iata,heard_at,path_length_byte,hash_size,hop_count,path_bytes,payload_type) VALUES
  ('\x01','00000000-0000-0000-0000-000000000001','YVR',NOW(),2,1,2,'\x28f4',9),
  ('\x01','00000000-0000-0000-0000-000000000002','YVR',NOW()+interval '1 second',0,1,0,'\x',9),
  ('\x01','00000000-0000-0000-0000-000000000003','YVR',NOW()+interval '2 seconds',0,1,0,NULL,9),
  ('\x02','00000000-0000-0000-0000-000000000001','YVR',NOW(),2,1,2,'\x7e9b',2)`); err != nil {
		t.Fatal(err)
	}
	store := &Store{q: sqlc.New(pool)}
	observations := func(hash byte) []map[string]any {
		t.Helper()
		p, err := store.GetPacket(ctx, []byte{hash})
		if err != nil {
			t.Fatal(err)
		}
		raw, err := json.Marshal(p)
		if err != nil {
			t.Fatal(err)
		}
		var got struct {
			Observations []map[string]any `json:"observations"`
		}
		if err := json.Unmarshal(raw, &got); err != nil {
			t.Fatal(err)
		}
		return got.Observations
	}

	trace := observations(1)
	if len(trace) != 3 {
		t.Fatalf("%d trace observations", len(trace))
	}
	for i, want := range []struct{ raw, wire string }{{"02", "28f4"}, {"00", ""}, {"00", ""}} {
		o := trace[i]
		wire, sent := o["wirePathBytes"]
		length := o["pathLength"].(map[string]any)
		if o["pathBytes"] != "a1b2c3" || !sent || wire != want.wire || length["raw"] != want.raw || length["hashSize"] != float64(1) || length["hopCount"] != float64(3) {
			t.Errorf("trace observation %d: pathBytes %v wirePathBytes %v (sent %v) pathLength %v", i, o["pathBytes"], wire, sent, length)
		}
	}
	// The frame the first observer heard, rebuilt the way the clients do it.
	if frame := "26" + trace[0]["pathLength"].(map[string]any)["raw"].(string) + trace[0]["wirePathBytes"].(string) + "112233440000000000a1b2c3"; frame != "260228f4112233440000000000a1b2c3" {
		t.Errorf("rebuilt frame %s", frame)
	}

	other := observations(2)
	if _, sent := other[0]["wirePathBytes"]; sent || other[0]["pathBytes"] != "7e9b" {
		t.Errorf("non-trace observation: %v", other[0])
	}
}
