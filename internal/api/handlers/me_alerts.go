// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"context"
	"encoding/hex"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/MeshCore-Beacon/beacon-server/internal/alerts"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	mw "github.com/MeshCore-Beacon/beacon-server/internal/api/middleware"
	"github.com/MeshCore-Beacon/beacon-server/internal/users"
)

// AlertsService is the alert rules and devices the routes call. It is *alerts.Service.
type AlertsService interface {
	Overview(ctx context.Context, userID uuid.UUID) (alerts.Overview, error)
	PutRule(ctx context.Context, userID uuid.UUID, in alerts.RuleInput) (alerts.StoredRule, error)
	DeleteRule(ctx context.Context, userID, id uuid.UUID) error
	RegisterDevice(ctx context.Context, userID, sessionID uuid.UUID, platform, token string) error
	RemoveDevice(ctx context.Context, userID, sessionID uuid.UUID) error
}

// AlertRoutes mounts /alerts and /devices on the /me router. A nil service answers 404 alerts_disabled.
// A push token is never logged or returned.
func AlertRoutes(svc AlertsService) MeMount {
	return func(r chi.Router, writes func(http.Handler) http.Handler) {
		if svc == nil {
			off := func(w http.ResponseWriter, _ *http.Request) {
				respondCode(w, http.StatusNotFound, "alerts_disabled", "Alerts are not enabled on this server.")
			}
			r.HandleFunc("/alerts", off)
			r.HandleFunc("/alerts/*", off)
			r.HandleFunc("/devices", off)
			return
		}
		r.Get("/alerts", getAlerts(svc))
		r.With(writes).Put("/alerts/rules", putAlertRule(svc))
		r.With(writes).Delete("/alerts/rules/{id}", deleteAlertRule(svc))
		// A push token is a possession secret, so registering is limited per user as well as per address.
		r.With(writes, mw.RateLimitUser(10, 10*time.Minute)).Post("/devices", postDevice(svc))
		r.With(writes).Delete("/devices", deleteDevice(svc))
	}
}

func alertError(w http.ResponseWriter, err error) {
	var unavailable *alerts.KindUnavailable
	var bad *alerts.InvalidThreshold
	switch {
	case errors.Is(err, alerts.ErrNotOnAtlas):
		respondCode(w, http.StatusConflict, "not_on_atlas", "Rules exist only for radios on your Atlas.")
	case errors.As(err, &unavailable):
		respond(w, http.StatusUnprocessableEntity, map[string]APIError{"error": {Code: "kind_unavailable", Message: unavailable.Reason,
			ReasonCode: unavailable.Code}})
	case errors.As(err, &bad):
		respondCode(w, http.StatusUnprocessableEntity, "invalid_threshold", bad.Message)
	case errors.Is(err, alerts.ErrBadDevice):
		respondCode(w, http.StatusUnprocessableEntity, "invalid_input", "Check the platform and the push token.")
	case errors.Is(err, alerts.ErrRuleMissing):
		respondCode(w, http.StatusNotFound, "not_found", "No such rule.")
	case errors.Is(err, users.ErrNotFound):
		respondCode(w, http.StatusUnauthorized, "not_signed_in", "Sign in first.")
	default:
		slog.Error("alerts request failed", "component", "api", "error", err)
		respondError(w, http.StatusInternalServerError, "internal error")
	}
}

func alertRule(r alerts.StoredRule) api.AlertRule {
	out := api.AlertRule{ID: r.ID.String(), Kind: r.Kind, Threshold: r.Threshold, ThresholdHigh: r.High, Enabled: r.Enabled,
		State: r.State, StateChangedAt: r.StateChangedAt.UnixMilli()}
	if r.LastSentAt != nil {
		ms := r.LastSentAt.UnixMilli()
		out.LastSentAt = &ms
	}
	return out
}

// getAlerts godoc
// @Summary The user's alerts
// @Description One target per Atlas pin, in Atlas order, with the kinds its radio can have, the reason for each it cannot, and its rules. hasDevice says whether an app session has a push token (push mode). 404 alerts_disabled when the server has alerts off.
// @Tags Accounts
// @Produce json
// @Security UserSession
// @Success 200 {object} api.AlertsResponse
// @Failure 401,404 {object} map[string]APIError
// @Router /me/alerts [get]
func getAlerts(svc AlertsService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		u, _, _ := mw.UserFrom(r.Context())
		ov, err := svc.Overview(r.Context(), u.ID)
		if err != nil {
			alertError(w, err)
			return
		}
		out := api.AlertsResponse{Delivery: ov.Delivery, HasDevice: ov.HasDevice, Targets: make([]api.AlertTarget, 0, len(ov.Targets))}
		for _, t := range ov.Targets {
			at := api.AlertTarget{Kind: t.Kind, PublicKey: t.PublicKey, Name: t.Name, Polled: t.Caps.Polled, Available: t.Available,
				Unavailable: t.Unavailable, UnavailableCodes: t.UnavailableCodes, Rules: make([]api.AlertRule, 0, len(t.Rules))}
			if at.Available == nil {
				at.Available = []string{}
			}
			if at.Unavailable == nil {
				at.Unavailable = map[string]string{}
			}
			if at.UnavailableCodes == nil {
				at.UnavailableCodes = map[string]string{}
			}
			if t.ID != uuid.Nil {
				id := t.ID.String()
				at.ID = &id
			}
			for _, rule := range t.Rules {
				at.Rules = append(at.Rules, alertRule(rule))
			}
			out.Targets = append(out.Targets, at)
		}
		respond(w, http.StatusOK, out)
	}
}

// putAlertRule godoc
// @Summary Create or change an alert rule
// @Description Upserts the rule for (target, kind). A change of thresholds, or turning a rule on, starts its state again. Thresholds are in the kind's own unit (see api.AlertRule). Answers 200 with the rule. back_online needs an enabled gone_quiet rule on the same radio.
// @Tags Accounts
// @Accept json
// @Produce json
// @Security UserSession
// @Param body body api.PutAlertRuleRequest true "Body at most 4 KiB"
// @Success 200 {object} api.AlertRule
// @Failure 400,401,403,413,415,429,503 {object} map[string]APIError
// @Failure 404 {object} map[string]APIError "alerts_disabled"
// @Failure 409 {object} map[string]APIError "not_on_atlas"
// @Failure 422 {object} map[string]APIError "invalid_input, kind_unavailable or invalid_threshold"
// @Router /me/alerts/rules [put]
func putAlertRule(svc AlertsService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var in api.PutAlertRuleRequest
		if !decodeBody(w, r, &in) {
			return
		}
		key, err := hex.DecodeString(in.Target.PublicKey)
		if err != nil || len(key) != 32 {
			respondCode(w, http.StatusUnprocessableEntity, "invalid_input", "target.publicKey must be 64 hex characters.")
			return
		}
		enabled := in.Enabled == nil || *in.Enabled
		u, _, _ := mw.UserFrom(r.Context())
		rule, err := svc.PutRule(r.Context(), u.ID, alerts.RuleInput{TargetKind: in.Target.Kind, PublicKey: key, Kind: in.Kind,
			Threshold: in.Threshold, High: in.ThresholdHigh, Enabled: enabled})
		if err != nil {
			alertError(w, err)
			return
		}
		respond(w, http.StatusOK, alertRule(rule))
	}
}

// deleteAlertRule godoc
// @Summary Delete an alert rule
// @Tags Accounts
// @Security UserSession
// @Param id path string true "Rule UUID" format(uuid)
// @Success 204 "Deleted"
// @Failure 401,403,404,422,429,503 {object} map[string]APIError
// @Router /me/alerts/rules/{id} [delete]
func deleteAlertRule(svc AlertsService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, err := uuid.Parse(chi.URLParam(r, "id"))
		if err != nil {
			respondCode(w, http.StatusUnprocessableEntity, "invalid_input", "Not a rule id.")
			return
		}
		u, _, _ := mw.UserFrom(r.Context())
		if err := svc.DeleteRule(r.Context(), u.ID, id); err != nil {
			alertError(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}

// appSessionOnly refuses a cookie session: a push token belongs to an app, which signs in with a bearer token.
func appSessionOnly(w http.ResponseWriter, r *http.Request) bool {
	if mw.SessionViaCookie(r.Context()) {
		respondCode(w, http.StatusForbidden, "app_session_required", "Only the app registers a device.")
		return false
	}
	return true
}

// postDevice godoc
// @Summary Register this app session's push token
// @Description Bearer sessions only (the app); a cookie session gets 403 app_session_required. Replaces the token of this session, and a token another session held moves here. Ending the session ends delivery to it.
// @Tags Accounts
// @Accept json
// @Security UserSession
// @Param body body api.RegisterDeviceRequest true "Body at most 4 KiB"
// @Success 204 "Registered"
// @Failure 400,401,403,413,415,429,503 {object} map[string]APIError
// @Failure 404 {object} map[string]APIError "alerts_disabled"
// @Failure 422 {object} map[string]APIError "invalid_input"
// @Router /me/devices [post]
func postDevice(svc AlertsService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !appSessionOnly(w, r) {
			return
		}
		var in api.RegisterDeviceRequest
		if !decodeBody(w, r, &in) {
			return
		}
		u, s, _ := mw.UserFrom(r.Context())
		if err := svc.RegisterDevice(r.Context(), u.ID, s.ID, in.Platform, in.PushToken); err != nil {
			alertError(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}

// deleteDevice godoc
// @Summary Remove this session's push token
// @Tags Accounts
// @Security UserSession
// @Success 204 "Removed, or there was none"
// @Failure 401,403,404,429,503 {object} map[string]APIError
// @Router /me/devices [delete]
func deleteDevice(svc AlertsService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !appSessionOnly(w, r) {
			return
		}
		u, s, _ := mw.UserFrom(r.Context())
		if err := svc.RemoveDevice(r.Context(), u.ID, s.ID); err != nil {
			alertError(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}
