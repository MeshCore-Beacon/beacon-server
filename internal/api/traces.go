package api

// TraceTagSummary is a single trace tag with aggregate metadata.
type TraceTagSummary struct {
	TraceTag     string `json:"traceTag"`     // hex-encoded 4-byte tag
	FirstHeardAt int64  `json:"firstHeardAt"` // epoch ms
	LastHeardAt  int64  `json:"lastHeardAt"`  // epoch ms
	PacketCount  int64  `json:"packetCount"`  // number of packets with this trace tag
	IATACount    int64  `json:"iataCount"`    // number of distinct IATAs where heard
}

// TracePacket is a single packet within a trace series, including its
// resolved route derived from the trace path hashes.
type TracePacket struct {
	PacketHash    string        `json:"packetHash"`
	RouteType     int16         `json:"routeType"`
	RouteTypeName string        `json:"routeTypeName"`
	Scope         *string       `json:"scope,omitempty"`
	FirstHeardAt  int64         `json:"firstHeardAt"` // epoch ms
	LastHeardAt   int64         `json:"lastHeardAt"`  // epoch ms
	ResolvedRoute []ResolvedHop `json:"resolvedRoute"`
}

// TraceDetail is the full trace series for a given trace tag.
type TraceDetail struct {
	TraceTag string        `json:"traceTag"` // hex-encoded 4-byte tag
	Packets  []TracePacket `json:"packets"`
}
