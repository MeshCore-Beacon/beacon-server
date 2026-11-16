// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later
package router

import (
	"encoding/json"
	"net/http/httptest"
	"testing"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
)

func TestInfoIncludesArtifactIdentityWithoutChangingAPIVersion(t *testing.T) {
	r := New(nil, nil, nil, Options{ServerVersion: "2.1.0", BuildVersion: "candidate", BuildRevision: "0123456789abcdef"})
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/info", nil))
	var info api.Info
	if err := json.Unmarshal(w.Body.Bytes(), &info); err != nil {
		t.Fatal(err)
	}
	if w.Code != 200 || info.ServerVersion != "2.1.0" || info.BuildVersion != "candidate" || info.BuildRevision != "0123456789abcdef" {
		t.Fatalf("info=%+v status=%d", info, w.Code)
	}
}
