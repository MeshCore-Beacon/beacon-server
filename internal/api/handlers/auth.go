// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"mime"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	mw "github.com/MeshCore-Beacon/beacon-server/internal/api/middleware"
	"github.com/MeshCore-Beacon/beacon-server/internal/users"
)

// SessionCookie is the web session cookie.
const SessionCookie = "beacon_session"

// AppRedirectURI is where the app sign-in sends the code; the server accepts no other.
const AppRedirectURI = "meshcorebeacon://auth"

// A browser keeps a cookie for 400 days at most. The server's own expiry is the real one and slides, so the
// cookie is simply long-lived rather than re-sent on every touch.
const cookieMaxAge = 400 * 24 * time.Hour

// AccountsService is what the sign-in and profile routes need from the accounts service.
type AccountsService interface {
	Authenticate(ctx context.Context, token string) (users.Session, users.User, error)
	Register(ctx context.Context, username, email, password string) error
	Verify(ctx context.Context, token string) error
	ResendVerification(ctx context.Context, email string) error
	Login(ctx context.Context, login, password string, meta users.SessionMeta) (string, users.Session, users.User, error)
	Logout(ctx context.Context, userID, sessionID uuid.UUID) error
	RequestReset(ctx context.Context, email string) error
	ConfirmReset(ctx context.Context, token, newPassword string) error
	IssueAppCode(ctx context.Context, u users.User, codeChallenge, deviceLabel string) (string, error)
	ExchangeAppCode(ctx context.Context, code, codeVerifier string, meta users.SessionMeta) (string, users.Session, users.User, error)
	Sessions(ctx context.Context, userID uuid.UUID) ([]users.Session, error)
	EndSession(ctx context.Context, userID, sessionID uuid.UUID) error
	EndOtherSessions(ctx context.Context, userID, keep uuid.UUID) error
	ChangePassword(ctx context.Context, u users.User, keepSession uuid.UUID, current, newPassword string) error
	ChangeEmail(ctx context.Context, u users.User, current, newEmail string) error
	SetDisplayName(ctx context.Context, u users.User, name string) (users.User, error)
	DeleteAccount(ctx context.Context, u users.User, current string) error
}

// AuthOptions is how the account routes behave. With Enabled false every path answers 404 accounts_disabled.
type AuthOptions struct {
	Enabled      bool
	CookieSecure bool
	// PublicOrigin is the origin of accounts.public_url (scheme and host); AllowedOrigins is cors.allowed_origins.
	PublicOrigin   string
	AllowedOrigins []string
	// ReadOnly makes every write route answer 503.
	ReadOnly bool
	// Portal is set in portal mode: the local sign-up and password routes answer 404 portal_managed and the portal
	// routes are served.
	Portal PortalAuth
}

func respondCode(w http.ResponseWriter, status int, code, message string) {
	respond(w, status, map[string]APIError{"error": {Code: code, Message: message}})
}

func accountsDisabled() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		respondCode(w, http.StatusNotFound, "accounts_disabled", "Accounts are not enabled on this server.")
	})
}

func isWrite(r *http.Request) bool {
	switch r.Method {
	case http.MethodGet, http.MethodHead, http.MethodOptions:
		return false
	}
	return true
}

// accountsGate marks every answer no-store (so no cache keeps a session or a user) and refuses writes on a
// read-only instance.
func accountsGate(opts AuthOptions) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Cache-Control", "no-store")
			if opts.ReadOnly && isWrite(r) {
				respondCode(w, http.StatusServiceUnavailable, "read_only", "This server is read-only.")
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// decodeBody reads one JSON object of at most 4 KiB with no unknown fields, as the admin accounts handler does.
// It answers the refusal itself and returns false.
func decodeBody(w http.ResponseWriter, r *http.Request, dst any) bool {
	return decodeBodyMax(w, r, dst, 4096)
}

// decodeBodyMax is decodeBody with a larger limit for the few routes that need one.
func decodeBodyMax(w http.ResponseWriter, r *http.Request, dst any, limit int64) bool {
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		respondError(w, http.StatusUnsupportedMediaType, "Content-Type must be application/json")
		return false
	}
	r.Body = http.MaxBytesReader(w, r.Body, limit)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	err = dec.Decode(dst)
	if err == nil {
		var extra any
		if err = dec.Decode(&extra); errors.Is(err, io.EOF) {
			err = nil
		} else if err == nil {
			err = errors.New("multiple JSON values")
		}
	}
	if err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			respondError(w, http.StatusRequestEntityTooLarge, fmt.Sprintf("request body exceeds %d KiB", limit/1024))
		} else {
			respondError(w, http.StatusBadRequest, "invalid JSON body")
		}
		return false
	}
	return true
}

// userError answers an error from the accounts service with the code clients act on.
func userError(w http.ResponseWriter, err error) {
	var tooMany *users.TooManyAttemptsError
	var policy *users.PolicyError
	switch {
	case errors.As(err, &tooMany):
		secs := int(math.Ceil(tooMany.RetryAfter.Seconds()))
		if secs < 1 {
			secs = 1
		}
		w.Header().Set("Retry-After", strconv.Itoa(secs))
		respondCode(w, http.StatusTooManyRequests, "too_many_attempts", "Too many failed attempts. Try again later.")
	case errors.Is(err, users.ErrTooManyAttempts):
		w.Header().Set("Retry-After", "900")
		respondCode(w, http.StatusTooManyRequests, "too_many_attempts", "Too many failed attempts. Try again later.")
	case errors.Is(err, users.ErrBusy):
		w.Header().Set("Retry-After", "5")
		respondCode(w, http.StatusServiceUnavailable, "service_unavailable", "The server is busy. Try again in a moment.")
	case errors.Is(err, users.ErrInvalidCredentials):
		respondCode(w, http.StatusUnauthorized, "invalid_credentials", "Wrong username, email or password.")
	case errors.Is(err, users.ErrEmailUnverified):
		respondCode(w, http.StatusForbidden, "email_unverified", "Confirm your email address first.")
	case errors.Is(err, users.ErrTokenInvalid):
		respondCode(w, http.StatusBadRequest, "token_invalid", "This link or code is invalid or has expired.")
	case errors.Is(err, users.ErrRegistrationClosed):
		respondCode(w, http.StatusForbidden, "registration_closed", "Registration is closed on this server.")
	case errors.Is(err, users.ErrUsernameTaken):
		respondCode(w, http.StatusConflict, "username_taken", "That username is not available.")
	case errors.As(err, &policy):
		respondCode(w, http.StatusUnprocessableEntity, "weak_password", policy.Reason)
	case errors.Is(err, users.ErrWeakPassword):
		respondCode(w, http.StatusUnprocessableEntity, "weak_password", "That password is not allowed.")
	case errors.Is(err, users.ErrInvalidInput):
		respondCode(w, http.StatusUnprocessableEntity, "invalid_input", "Check the values you entered.")
	default:
		respondError(w, http.StatusInternalServerError, "internal error")
	}
}

func setSessionCookie(w http.ResponseWriter, opts AuthOptions, token string) {
	http.SetCookie(w, &http.Cookie{
		Name: SessionCookie, Value: token, Path: "/", MaxAge: int(cookieMaxAge.Seconds()),
		HttpOnly: true, Secure: opts.CookieSecure, SameSite: http.SameSiteLaxMode,
	})
}

func clearSessionCookie(w http.ResponseWriter, opts AuthOptions) {
	http.SetCookie(w, &http.Cookie{
		Name: SessionCookie, Value: "", Path: "/", MaxAge: -1,
		HttpOnly: true, Secure: opts.CookieSecure, SameSite: http.SameSiteLaxMode,
	})
}

func clientIP(r *http.Request) string {
	if host, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
		return host
	}
	return r.RemoteAddr
}

const maxSessionLabel = 60

// browserLabel turns a User-Agent into "Firefox on macOS", "Web browser" when the browser is unknown.
func browserLabel(ua string) string {
	family := ""
	switch {
	case strings.Contains(ua, "Edg/") || strings.Contains(ua, "EdgA/") || strings.Contains(ua, "EdgiOS/"):
		family = "Edge"
	case strings.Contains(ua, "OPR/") || strings.Contains(ua, "Opera"):
		family = "Opera"
	case strings.Contains(ua, "Firefox/") || strings.Contains(ua, "FxiOS/"):
		family = "Firefox"
	case strings.Contains(ua, "Chrome/") || strings.Contains(ua, "CriOS/"):
		family = "Chrome"
	case strings.Contains(ua, "Safari/") && strings.Contains(ua, "Version/"):
		family = "Safari"
	}
	if family == "" {
		return "Web browser"
	}
	osName := ""
	switch {
	case strings.Contains(ua, "Android"):
		osName = "Android"
	case strings.Contains(ua, "iPhone") || strings.Contains(ua, "iPad") || strings.Contains(ua, "iPod"):
		osName = "iOS"
	case strings.Contains(ua, "Windows"):
		osName = "Windows"
	case strings.Contains(ua, "Mac OS X") || strings.Contains(ua, "Macintosh"):
		osName = "macOS"
	case strings.Contains(ua, "CrOS"):
		osName = "ChromeOS"
	case strings.Contains(ua, "Linux") || strings.Contains(ua, "X11"):
		osName = "Linux"
	}
	label := family
	if osName != "" {
		label += " on " + osName
	}
	if len(label) > maxSessionLabel {
		label = label[:maxSessionLabel]
	}
	return label
}

func sessionMeta(r *http.Request) users.SessionMeta {
	ua := r.Header.Get("User-Agent")
	return users.SessionMeta{IP: clientIP(r), UserAgent: ua, Label: browserLabel(ua)}
}

func checkEmail() api.StatusResponse { return api.StatusResponse{Status: "check_email"} }

// AuthRouter serves /api/v1/auth. It expects mw.UserAuth upstream for the routes that need a session.
func AuthRouter(svc AccountsService, opts AuthOptions) http.Handler {
	if !opts.Enabled || svc == nil {
		return accountsDisabled()
	}
	r := chi.NewRouter()
	r.Use(accountsGate(opts))
	r.Use(mw.RequireCSRF(opts.PublicOrigin, opts.AllowedOrigins))
	minute := time.Minute
	five := 5 * time.Minute
	other := mw.RateLimitRoute(60, minute) // shared by the session routes
	if opts.Portal != nil {
		for _, p := range []string{"/register", "/verify", "/verify/resend", "/login", "/reset/request", "/reset/confirm"} {
			r.Post(p, portalManaged)
		}
		r.With(mw.RateLimitRoute(20, minute)).Get("/portal/start", portalStart(opts))
		r.With(mw.RateLimitRoute(20, minute)).Get("/portal/callback", portalCallback(opts))
	} else {
		r.With(mw.RateLimitRoute(5, five)).Post("/register", register(svc))
		r.With(mw.RateLimitRoute(30, minute)).Post("/verify", verify(svc))
		r.With(mw.RateLimitRoute(5, five)).Post("/verify/resend", resendVerify(svc))
		r.With(mw.RateLimitRoute(10, minute)).Post("/login", login(svc, opts))
		r.With(mw.RateLimitRoute(5, five)).Post("/reset/request", resetRequest(svc))
		r.With(mw.RateLimitRoute(10, five)).Post("/reset/confirm", resetConfirm(svc))
	}
	r.With(other, mw.RequireUser).Post("/logout", logout(svc, opts))
	r.With(other, mw.RequireUser).Post("/app/code", appCode(svc))
	r.With(mw.RateLimitRoute(10, minute)).Post("/app/token", appToken(svc))
	return r
}

// register godoc
// @Summary Create an account
// @Description Always answers 202 check_email for a well-formed request, whether or not the address is already registered; a verify link is mailed.
// @Tags Accounts
// @Accept json
// @Produce json
// @Param body body api.RegisterRequest true "Username 3-30 of A-Za-z0-9_.-, email, password 12-128 bytes; body at most 4 KiB"
// @Success 202 {object} api.StatusResponse
// @Failure 400,403,409,413,415,422,429,503 {object} map[string]APIError
// @Router /auth/register [post]
func register(svc AccountsService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var in api.RegisterRequest
		if !decodeBody(w, r, &in) {
			return
		}
		if err := svc.Register(r.Context(), in.Username, in.Email, in.Password); err != nil {
			userError(w, err)
			return
		}
		respond(w, http.StatusAccepted, checkEmail())
	}
}

// verify godoc
// @Summary Confirm an email address
// @Description Uses the one-time token from the mailed link. Also completes an email change.
// @Tags Accounts
// @Accept json
// @Produce json
// @Param body body api.TokenRequest true "Token from the link; body at most 4 KiB"
// @Success 200 {object} api.StatusResponse
// @Failure 400,413,415,429,503 {object} map[string]APIError
// @Router /auth/verify [post]
func verify(svc AccountsService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var in api.TokenRequest
		if !decodeBody(w, r, &in) {
			return
		}
		if err := svc.Verify(r.Context(), in.Token); err != nil {
			userError(w, err)
			return
		}
		respond(w, http.StatusOK, api.StatusResponse{Status: "verified"})
	}
}

// resendVerify godoc
// @Summary Send the verify link again
// @Description Answers 202 check_email whether or not the address belongs to an unverified account.
// @Tags Accounts
// @Accept json
// @Produce json
// @Param body body api.EmailRequest true "Email address; body at most 4 KiB"
// @Success 202 {object} api.StatusResponse
// @Failure 400,413,415,422,429,503 {object} map[string]APIError
// @Router /auth/verify/resend [post]
func resendVerify(svc AccountsService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var in api.EmailRequest
		if !decodeBody(w, r, &in) {
			return
		}
		if err := svc.ResendVerification(r.Context(), in.Email); err != nil {
			userError(w, err)
			return
		}
		respond(w, http.StatusAccepted, checkEmail())
	}
}

// login godoc
// @Summary Sign in
// @Description Sets the beacon_session cookie (HttpOnly, SameSite=Lax) and returns the user. One generic invalid_credentials answer covers an unknown name and a wrong password; email_unverified comes only after the password is right.
// @Tags Accounts
// @Accept json
// @Produce json
// @Param body body api.LoginRequest true "Username or email, and password; body at most 4 KiB"
// @Success 200 {object} api.UserResponse
// @Failure 400,401,403,413,415,429,503 {object} map[string]APIError
// @Router /auth/login [post]
func login(svc AccountsService, opts AuthOptions) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var in api.LoginRequest
		if !decodeBody(w, r, &in) {
			return
		}
		token, _, u, err := svc.Login(r.Context(), in.Login, in.Password, sessionMeta(r))
		if err != nil {
			userError(w, err)
			return
		}
		setSessionCookie(w, opts, token)
		respond(w, http.StatusOK, api.UserResponse{User: api.NewUser(u)})
	}
}

// logout godoc
// @Summary Sign out
// @Tags Accounts
// @Security UserSession
// @Success 204 "Signed out; the cookie is cleared"
// @Failure 401,403,429,503 {object} map[string]APIError
// @Router /auth/logout [post]
func logout(svc AccountsService, opts AuthOptions) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		u, s, _ := mw.UserFrom(r.Context())
		if err := svc.Logout(r.Context(), u.ID, s.ID); err != nil {
			userError(w, err)
			return
		}
		clearSessionCookie(w, opts)
		w.WriteHeader(http.StatusNoContent)
	}
}

// resetRequest godoc
// @Summary Ask for a password reset link
// @Description Answers 202 check_email whether or not the address belongs to an account.
// @Tags Accounts
// @Accept json
// @Produce json
// @Param body body api.EmailRequest true "Email address; body at most 4 KiB"
// @Success 202 {object} api.StatusResponse
// @Failure 400,413,415,422,429,503 {object} map[string]APIError
// @Router /auth/reset/request [post]
func resetRequest(svc AccountsService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var in api.EmailRequest
		if !decodeBody(w, r, &in) {
			return
		}
		if err := svc.RequestReset(r.Context(), in.Email); err != nil {
			userError(w, err)
			return
		}
		respond(w, http.StatusAccepted, checkEmail())
	}
}

// resetConfirm godoc
// @Summary Set a new password with a reset link
// @Description Ends every session of the user. A refused password does not use up the link.
// @Tags Accounts
// @Accept json
// @Produce json
// @Param body body api.ResetConfirmRequest true "Token from the link and the new password; body at most 4 KiB"
// @Success 200 {object} api.StatusResponse
// @Failure 400,413,415,422,429,503 {object} map[string]APIError
// @Router /auth/reset/confirm [post]
func resetConfirm(svc AccountsService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var in api.ResetConfirmRequest
		if !decodeBody(w, r, &in) {
			return
		}
		if err := svc.ConfirmReset(r.Context(), in.Token, in.Password); err != nil {
			userError(w, err)
			return
		}
		respond(w, http.StatusOK, api.StatusResponse{Status: "password_set"})
	}
}

// appCode godoc
// @Summary Issue a one-time code for the app
// @Description Needs the web cookie session. The code lasts 5 minutes and is bound to the PKCE challenge (S256). redirectUri is always meshcorebeacon://auth.
// @Tags Accounts
// @Accept json
// @Produce json
// @Param body body api.AppCodeRequest true "PKCE challenge, state and device label; body at most 4 KiB"
// @Success 200 {object} api.AppCodeResponse
// @Failure 400,401,403,413,415,422,429,503 {object} map[string]APIError
// @Router /auth/app/code [post]
func appCode(svc AccountsService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !mw.SessionViaCookie(r.Context()) {
			respondCode(w, http.StatusUnauthorized, "not_signed_in", "Sign in on the web to link an app.")
			return
		}
		var in api.AppCodeRequest
		if !decodeBody(w, r, &in) {
			return
		}
		if len(in.State) > 256 {
			userError(w, users.ErrInvalidInput)
			return
		}
		u, _, _ := mw.UserFrom(r.Context())
		code, err := svc.IssueAppCode(r.Context(), u, in.CodeChallenge, in.DeviceLabel)
		if err != nil {
			userError(w, err)
			return
		}
		respond(w, http.StatusOK, api.AppCodeResponse{Code: code, RedirectURI: AppRedirectURI})
	}
}

// appToken godoc
// @Summary Exchange an app code for a session token
// @Description The code is used up before the verifier is checked, so a wrong verifier needs a new code. The token is for the Authorization: Bearer header.
// @Tags Accounts
// @Accept json
// @Produce json
// @Param body body api.AppTokenRequest true "Code and PKCE verifier; body at most 4 KiB"
// @Success 200 {object} api.AppTokenResponse
// @Failure 400,413,415,429,503 {object} map[string]APIError
// @Router /auth/app/token [post]
func appToken(svc AccountsService) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var in api.AppTokenRequest
		if !decodeBody(w, r, &in) {
			return
		}
		meta := users.SessionMeta{IP: clientIP(r), UserAgent: r.Header.Get("User-Agent")}
		token, s, u, err := svc.ExchangeAppCode(r.Context(), in.Code, in.CodeVerifier, meta)
		if err != nil {
			userError(w, err)
			return
		}
		respond(w, http.StatusOK, api.AppTokenResponse{Token: token, ExpiresAt: s.ExpiresAt.UnixMilli(), User: api.NewUser(u)})
	}
}
