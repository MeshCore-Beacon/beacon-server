// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package config

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/MeshCore-Beacon/beacon-server/internal/keystore"
)

func loadText(t *testing.T, text string) (*Config, error) {
	t.Helper()
	p := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(p, []byte(text), 0600); err != nil {
		t.Fatal(err)
	}
	return Load(p)
}

func TestChannelKeysRegionScoping(t *testing.T) {
	text := `regions:
  - slug: east
    iatas: [YOW]
channel_keys:
  hashtags:
    - meshcore
    - name: ottawa-mesh
      region: east
  keys:
    "11":
      key: "8b3387e9c5cdea6ac9e5edbaa115cd72"
      name: Public
`
	cfg, err := loadText(t, text)
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.ChannelKeys.Hashtags; len(got) != 2 || got[0].Name != "meshcore" || got[0].Region != "" || got[1].Name != "ottawa-mesh" || got[1].Region != "east" {
		t.Fatalf("hashtags parsed as %+v", got)
	}
	scopes := cfg.ChannelScopes()
	if len(scopes) != 3 {
		t.Fatalf("scopes: %+v", scopes)
	}
	_, _, ottawa := keystore.DeriveHashtagKey("ottawa-mesh")
	found := false
	for _, s := range scopes {
		if bytes.Equal(s.Fingerprint, ottawa) {
			found = s.Region == "east"
		}
	}
	if !found {
		t.Fatal("region-scoped hashtag missing its region", scopes)
	}

	for name, bad := range map[string]string{
		"unknown hashtag region": strings.Replace(text, "region: east", "region: west", 1),
		"empty hashtag":          strings.Replace(text, "- meshcore", `- ""`, 1),
		"unknown key region":     text + "      region: west\n",
	} {
		if _, err := loadText(t, bad); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
}

func TestMeshMapperChannelsConfig(t *testing.T) {
	valid := "meshmapper:\n  channels:\n    enabled: true\n"
	for text, bad := range map[string]bool{
		valid:                                  false,
		valid + "    refresh_interval: 24h\n":  false,
		valid + "    refresh_interval: 168h\n": false,
		valid + "    refresh_interval: 23h\n":  true,
		valid + "    refresh_interval: 169h\n": true,
	} {
		if _, err := loadText(t, text); (err != nil) != bad {
			t.Errorf("%q: error=%v", text, err)
		}
	}
	if (MeshMapperChannelsConfig{}).Interval() != MinChannelsRefresh {
		t.Fatal("wrong default")
	}
}
