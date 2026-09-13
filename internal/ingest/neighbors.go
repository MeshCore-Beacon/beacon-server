// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package ingest

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// neighborReportEntry is one entry in the "neighbors" array of a /neighbors report.
type neighborReportEntry struct {
	PubKey       string  `json:"pubkey"`
	SNR          float32 `json:"snr"`
	HeardSecsAgo int64   `json:"heard_secs_ago"`
	Scopes       string  `json:"scopes"`
	Status       string  `json:"status"` // "responded" or "timeout"
}

// neighborReport is the JSON payload for an observer /neighbors message.
type neighborReport struct {
	Self struct {
		Scopes string `json:"scopes"`
	} `json:"self"`
	Neighbors []neighborReportEntry `json:"neighbors"`
}

// handleNeighbors processes a /neighbors message: the observer's own OTA-configured
// region scope, plus a snapshot of its zero-hop neighbors and (best-effort) their
// OTA-queried region scopes. See the package doc comment for the full pipeline.
func (w *Worker) handleNeighbors(ctx context.Context, iata, pubkeyHex string, raw []byte) {
	var report neighborReport
	if err := json.Unmarshal(raw, &report); err != nil {
		w.log.Warn(fmt.Sprintf("malformed neighbors envelope from %s", pubkeyHex), "error", err)
		return
	}

	pubkey, err := hex.DecodeString(pubkeyHex)
	if err != nil {
		w.log.Warn(fmt.Sprintf("invalid pubkey hex in neighbors from %s", pubkeyHex), "error", err)
		return
	}

	observerID, _, err := w.db.UpsertObserver(ctx, pubkey)
	if err != nil {
		w.log.Error(fmt.Sprintf("db: upsert observer failed in neighbors from %s", pubkeyHex), "error", err)
		return
	}
	if err := w.db.UpsertObserverBroker(ctx, observerID, w.cfg.BrokerName); err != nil {
		w.log.Error(fmt.Sprintf("db: upsert observer broker failed in neighbors from %s", pubkeyHex), "error", err)
	}

	// self.scopes is always known (it's the observer's own config, not an OTA
	// query), so this is an unconditional write -- unlike the per-neighbor
	// scopes below, there's no "query failed" case to protect against here.
	if err := w.db.UpdateObserverRegionScope(ctx, observerID, report.Self.Scopes); err != nil {
		w.log.Error(fmt.Sprintf("db: update observer region scope failed for %s", pubkeyHex), "error", err)
	}

	// The observer's own node row (as opposed to its observers row above) is
	// what node_neighbors edges hang off of. As elsewhere in this package
	// (see GetNodeByPubkey's doc comment), an observer that hasn't advertised
	// yet has no node row, in which case we simply can't record edges for it
	// this round -- not an error, just nothing to attach the neighbors to.
	observerNodeID, err := w.db.GetNodeByPubkey(ctx, pubkey)
	if err != nil {
		return
	}

	for _, n := range report.Neighbors {
		neighborPubkey, err := hex.DecodeString(n.PubKey)
		if err != nil {
			w.log.Warn(fmt.Sprintf("invalid neighbor pubkey hex %q from %s", n.PubKey, pubkeyHex), "error", err)
			continue
		}
		neighborNodeID, err := w.db.GetNodeByPubkey(ctx, neighborPubkey)
		if err != nil {
			// Neighbor hasn't advertised yet -- nothing to link to. Its absence
			// here doesn't mean it's gone; it'll show up once it advertises.
			continue
		}
		if neighborNodeID == observerNodeID {
			continue // don't record a node as its own neighbor
		}

		snr := n.SNR

		// OTA region scope queries are unreliable (per the firmware author, even
		// the mobile app sees this) -- only "responded" is a trustworthy read.
		// A "timeout" must not be taken as "the neighbor cleared its scope", so
		// we pass nil and let UpsertNodeNeighbor's COALESCE preserve whatever
		// was last known.
		var regionScope *string
		if n.Status == "responded" {
			regionScope = &n.Scopes
		}

		if err := w.db.UpsertNodeNeighbor(ctx, observerNodeID, neighborNodeID, iata, &snr, regionScope); err != nil {
			w.log.Error(fmt.Sprintf("db: upsert neighbor failed for %s -> %s", pubkeyHex, n.PubKey), "error", err)
		}
	}
}
