package meshmapper

import (
	"context"
	"crypto/sha256"
	"github.com/MeshCore-Beacon/beacon-server/internal/config"
	"strings"
	"testing"
	"time"
)

func TestProfileEvidenceAndBounds(t *testing.T) {
	rows, _, err := decodeRepeaters([]byte(repeaterDocExample))
	if err != nil {
		t.Fatal(err)
	}
	p := rows[0].Data
	if p.Status != "active" || p.FirstSeen == nil || *p.FirstSeen != 1773103950000 || p.AdvertBytes == nil || *p.AdvertBytes != 2 || p.Backbone == nil || !*p.Backbone || p.BackboneShare == nil || len(p.Radios) != 1 || len(p.Neighbors) != 1 || p.Neighbors[0].Resolved {
		t.Fatalf("profile evidence lost: %+v", p)
	}
	if rows[0].locator != "A1,45.51235,-75.91235" {
		t.Fatal(rows[0].locator)
	}
	body := `[{"hex_id":"` + repeaterKey + `","enabled":77,"advert_bytes":4,"backbone":9,"backbone_share":1.1,"preset_current":"NaN,62.5,7","radios":[{"preset":"910.525,62.5,7","witnesses":999},{"preset":"bad"}],"proven_neighbours":[{"key":"ABC","resolved":1},{"key":"0123456789ABCDEF","resolved":1,"snr":100}]}]`
	rows, _, err = decodeRepeaters([]byte(body))
	if err != nil {
		t.Fatal(err)
	}
	p = rows[0].Data
	if p.Status != "" || p.AdvertBytes != nil || p.Backbone != nil || p.BackboneShare != nil || p.CurrentPreset != nil || len(p.Radios) != 1 || p.Radios[0].Witnesses != nil || len(p.Neighbors) != 1 || p.Neighbors[0].Resolved || p.Neighbors[0].SNR != nil {
		t.Fatalf("invalid evidence retained: %+v", p)
	}
}

func TestProfileRevocationAndAccessChange(t *testing.T) {
	for _, status := range []int{401, 403} {
		f := newRepeaterSite(t)
		s := newRepeaterMemoryStore("YOW")
		h := newRepeaterImporter(t, f, s, newZoneListMemory(), true)
		h.refreshN(t, 2)
		f.status = status
		h.due("YOW")
		h.refreshN(t, 1)
		if len(s.repeaters["YOW"]) != 0 {
			t.Fatalf("HTTP %d kept revoked profiles", status)
		}
	}
	f := newRepeaterSite(t)
	s := newRepeaterMemoryStore("YOW")
	lists := newZoneListMemory()
	h := newRepeaterImporter(t, f, s, lists, true)
	h.refreshN(t, 2)
	due := s.rows["YOW"].cache.NextAttempt
	d := newTestDirectory(lists)
	d.accessFingerprint = sha256.Sum256([]byte("different-test-key"))
	if _, err := NewRepeaters(context.Background(), config.MeshMapperRepeatersConfig{Enabled: true}, s, d, nil); err != nil {
		t.Fatal(err)
	}
	if len(s.repeaters) != 0 || !s.rows["YOW"].cache.NextAttempt.Equal(due) {
		t.Fatal("key change kept data or reset cooldown")
	}
}

func TestProfileRegionLimit(t *testing.T) {
	f := newRepeaterSite(t, repeaterZone{"YOW", "ONQC"}, repeaterZone{"YYZ", "ONQC"})
	// Group response includes an unauthorized/nonselected member as well.
	other := strings.ReplaceAll(strings.ReplaceAll(repeaterDocExample, repeaterKey, strings.Repeat("B", 64)), `"iata": "YOW"`, `"iata": "YYZ"`)
	f.body = strings.TrimSuffix(repeaterDocExample, "]") + "," + strings.TrimPrefix(other, "[")
	s := newRepeaterMemoryStore("YOW", "YYZ")
	d := newTestDirectory(newZoneListMemory())
	d.listURL = f.URL + "/get_zones.php"
	h, err := NewRepeaters(context.Background(), config.MeshMapperRepeatersConfig{Enabled: true, IATAs: []string{"YOW"}}, s, d, nil)
	if err != nil {
		t.Fatal(err)
	}
	h.repeatersURL = func(site string) (string, bool) { return site + "get_repeaters.php", true }
	for range 3 {
		if err = h.Refresh(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if len(s.repeaters["ONQC"]) != 1 || s.repeaters["ONQC"][0].Data.IATA != "YOW" || len(h.regions) != 1 {
		t.Fatal("import escaped selected region")
	}
	if s.rows["ONQC"].cache.NextAttempt.Before(time.Now()) {
		t.Fatal("unpaced import")
	}
}
