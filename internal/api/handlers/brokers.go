package handlers

import (
	"net/http"

	"github.com/MeshCore-Beacon/beacon-server/internal/ingest"
	"github.com/go-chi/chi/v5"
)

// BrokerStatus is the response shape for a single MQTT broker.
type BrokerStatus struct {
	Name      string `json:"name"`
	Connected bool   `json:"connected"`
}

// BrokersRouter mounts all /brokers routes onto a subrouter.
//
// GET  /brokers → listBrokers
//
// Note: broker configuration is managed via the server config
// file, not the API (v1). These endpoints are read-only.
func BrokersRouter(workers []*ingest.Worker) http.Handler {
	r := chi.NewRouter()
	r.Get("/", listBrokers(workers))
	return r
}

// listBrokers godoc
//
//	@Summary	List all MQTT brokers and their connection status
//	@Tags		Brokers
//	@Produce	json
//	@Success	200	{array}	handlers.BrokerStatus
//	@Router		/brokers [get]
func listBrokers(workers []*ingest.Worker) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		brokers := make([]BrokerStatus, len(workers))
		for i, v := range workers {
			brokers[i] = BrokerStatus{
				Name:      v.BrokerName(),
				Connected: v.IsConnected(),
			}
		}
		respond(w, http.StatusOK, brokers)
	}
}
