package api

import "github.com/google/uuid"

// RouteHop is a single resolved hop in a known route.
type RouteHop struct {
	NodeID    uuid.UUID     `json:"nodeId"`
	HashBytes string        `json:"hashBytes"`      // hex-encoded hash prefix
	Node      *ResolvedNode `json:"node,omitempty"` // populated when node details are available
}

// KnownRoute is a fully resolved path through the mesh where all hops
// have been confirmed as high confidence.
type KnownRoute struct {
	ID        int64      `json:"id"`
	IATA      string     `json:"iata"`
	HopCount  int32      `json:"hopCount"`
	Hops      []RouteHop `json:"hops"`
	FirstSeen int64      `json:"firstSeen"` // epoch ms
	LastSeen  int64      `json:"lastSeen"`  // epoch ms
}
