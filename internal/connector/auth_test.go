package connector

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/livekit/protocol/auth"
)

func TestAuthMiddleware(t *testing.T) {
	const key, secret = "test-key", "test-secret-long-enough"
	okToken, _ := auth.NewAccessToken(key, secret).SetVideoGrant(&auth.VideoGrant{RoomCreate: true}).ToJWT()
	badGrant, _ := auth.NewAccessToken(key, secret).SetVideoGrant(&auth.VideoGrant{}).ToJWT()
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	cases := map[string]struct {
		token  string
		status int
	}{
		"valid":     {okToken, http.StatusNoContent},
		"missing":   {"", http.StatusUnauthorized},
		"grant":     {badGrant, http.StatusUnauthorized},
		"signature": {okToken + "x", http.StatusUnauthorized},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodPost, "/", nil)
			if tc.token != "" {
				r.Header.Set("Authorization", "Bearer "+tc.token)
			}
			w := httptest.NewRecorder()
			AuthMiddleware(key, secret, next).ServeHTTP(w, r)
			if w.Code != tc.status {
				t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
			}
		})
	}
}
