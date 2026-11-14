// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package polling

// lppValueSize mirrors the firmware's table. -1 means an unknown type.
func lppValueSize(t byte) int {
	switch t {
	case 0, 1, 102, 120, 142:
		return 1
	case 2, 3, 101, 103, 104, 115, 116, 117, 121, 125, 128, 132:
		return 2
	case 135:
		return 3
	case 100, 118, 130, 131, 133:
		return 4
	case 113, 134:
		return 6
	case 136:
		return 9
	}
	return -1
}

const (
	lppChannelSelf = 1
	lppVoltage     = 116
	lppTemperature = 103
	lppTempMin     = -600 // -60.0 C
	lppTempMax     = 1250 // 125.0 C
)

// DecodeLPP reads the repeater's own voltage and temperature (channel 1) from raw
// CayenneLPP. It stops at channel 0, an unknown type or a truncated record and
// keeps what it decoded. The first record of each kind wins, even if out of range.
func DecodeLPP(b []byte) (batteryMV *int, temperatureC *float64) {
	sawV, sawT := false, false
	for i := 0; i+2 <= len(b); {
		ch, typ := b[i], b[i+1]
		if ch == 0 {
			break
		}
		sz := lppValueSize(typ)
		if sz < 0 || i+2+sz > len(b) {
			break
		}
		v := b[i+2 : i+2+sz]
		switch {
		case ch == lppChannelSelf && typ == lppVoltage && !sawV:
			sawV = true
			mv := int(uint16(v[0])<<8|uint16(v[1])) * 10
			if mv <= 65535 {
				batteryMV = &mv
			}
		case ch == lppChannelSelf && typ == lppTemperature && !sawT:
			sawT = true
			c10 := int(int16(uint16(v[0])<<8 | uint16(v[1])))
			if c10 >= lppTempMin && c10 <= lppTempMax {
				c := float64(c10) / 10
				temperatureC = &c
			}
		}
		i += 2 + sz
	}
	return batteryMV, temperatureC
}
