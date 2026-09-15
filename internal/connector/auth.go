package connector

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/livekit/protocol/auth"
)

func AuthMiddleware(apiKey, apiSecret string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		value := r.Header.Get("Authorization")
		if !strings.HasPrefix(value, "Bearer ") {
			writeAuthError(w, "missing bearer token")
			return
		}
		v, err := auth.ParseAPIToken(strings.TrimSpace(strings.TrimPrefix(value, "Bearer ")))
		if err != nil || v.APIKey() != apiKey {
			writeAuthError(w, "invalid bearer token")
			return
		}
		_, grants, err := v.Verify(apiSecret)
		if err != nil || grants == nil || grants.Video == nil || !grants.Video.RoomCreate {
			writeAuthError(w, "roomCreate permission required")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func writeAuthError(w http.ResponseWriter, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusUnauthorized)
	_ = json.NewEncoder(w).Encode(map[string]string{"code": "unauthenticated", "msg": message})
}
