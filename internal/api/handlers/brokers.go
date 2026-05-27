package handlers

import (
	"net/http"

	"github.com/MeshCore-Tower/tower-server/internal/ingest"
	"github.com/go-chi/chi/v5"
)

type BrokerStatus struct {
	Name      string `json:"name"`
	Connected bool   `json:"connected"`
}

// BrokersRouter mounts all /brokers routes onto a subrouter.
//
// GET  /brokers                              → ListBrokers
//
// Note: broker configuration is managed via the server config
// file, not the API (v1). These endpoints are read-only.
func BrokersRouter(workers []*ingest.Worker) http.Handler {
	r := chi.NewRouter()

	// GET  /brokers                              → ListBrokers
	//
	// Returns all configured brokers and their connection status
	r.Get("/", func(w http.ResponseWriter, r *http.Request) {
		brokers := make([]BrokerStatus, len(workers))
		for i, v := range workers {
			brokers[i] = BrokerStatus{
				Name:      v.BrokerName(),
				Connected: v.IsConnected(),
			}
		}
		respond(w, http.StatusOK, brokers)
	})
	return r
}
