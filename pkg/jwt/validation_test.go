/*
 * Copyright 2026 InfAI (CC SES)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package jwt

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"math/big"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

const testKid = "test-kid"

var (
	testKeysOnce  sync.Once
	testSignKey   *rsa.PrivateKey
	testOtherKey  *rsa.PrivateKey
	testCertDER   []byte
	testKeysError error
)

func testKeys(t *testing.T) {
	t.Helper()
	testKeysOnce.Do(func() {
		if testSignKey, testKeysError = rsa.GenerateKey(rand.Reader, 2048); testKeysError != nil {
			return
		}
		if testOtherKey, testKeysError = rsa.GenerateKey(rand.Reader, 2048); testKeysError != nil {
			return
		}
		template := &x509.Certificate{
			SerialNumber: big.NewInt(1),
			Subject:      pkix.Name{CommonName: "master"},
			NotBefore:    time.Now().Add(-time.Hour),
			NotAfter:     time.Now().Add(time.Hour),
		}
		testCertDER, testKeysError = x509.CreateCertificate(rand.Reader, template, template, &testSignKey.PublicKey, testSignKey)
	})
	if testKeysError != nil {
		t.Fatal(testKeysError)
	}
}

// certServer serves the signing key's certificate the way Keycloak's certs endpoint does and counts the downloads.
func certServer(t *testing.T) (*KeycloakCertProvider, *atomic.Int32) {
	t.Helper()
	testKeys(t)
	hits := &atomic.Int32{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		certs := map[string]any{"keys": []map[string]any{{
			"kid": testKid, "kty": "RSA", "alg": "RS256", "use": "sig",
			"x5c": []string{base64.StdEncoding.EncodeToString(testCertDER)},
		}}}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(certs)
	}))
	t.Cleanup(server.Close)
	return &KeycloakCertProvider{CertUrl: server.URL}, hits
}

func sign(t *testing.T, method jwt.SigningMethod, key any, claims jwt.MapClaims) string {
	t.Helper()
	token := jwt.NewWithClaims(method, claims)
	token.Header["kid"] = testKid
	signed, err := token.SignedString(key)
	if err != nil {
		t.Fatal(err)
	}
	return signed
}

func validClaims() jwt.MapClaims {
	now := time.Now()
	return jwt.MapClaims{
		"sub":                "u1",
		"preferred_username": "user1",
		"realm_access":       map[string][]string{"roles": {"user"}},
		"aud":                "frontend",
		"iat":                now.Unix(),
		"exp":                now.Add(5 * time.Minute).Unix(),
	}
}

func withClaim(claims jwt.MapClaims, key string, value any) jwt.MapClaims {
	if value == nil {
		delete(claims, key)
	} else {
		claims[key] = value
	}
	return claims
}

func TestParseWithValidationAcceptsValidToken(t *testing.T) {
	testKeys(t)
	tests := []struct {
		name   string
		method jwt.SigningMethod
		prefix string
	}{
		{name: "RS256 with bearer prefix", method: jwt.SigningMethodRS256, prefix: "Bearer "},
		{name: "RS256 without prefix", method: jwt.SigningMethodRS256},
		{name: "PS256", method: jwt.SigningMethodPS256, prefix: "bearer "},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			provider, _ := certServer(t)
			raw := test.prefix + sign(t, test.method, testSignKey, validClaims())
			token, err := ParseWithValidation(provider, raw)
			if err != nil {
				t.Fatal(err)
			}
			if token.Token != raw {
				t.Errorf("expected Token to keep the original header value, got %q", token.Token)
			}
			if token.Sub != "u1" || token.Username != "user1" || !token.HasRole("user") || !token.HasAudience("frontend") {
				t.Errorf("unexpected claims %#v", token)
			}
		})
	}
}

func TestParseWithValidationRejectsInvalidToken(t *testing.T) {
	testKeys(t)
	ecKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	publicDER, err := x509.MarshalPKIXPublicKey(&testSignKey.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	publicPEM := pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: publicDER})

	tests := []struct {
		name string
		// token builds the raw token; it runs after the provider exists so it may use the signing key.
		token func(t *testing.T) string
		// noCertFetch is set where the alg must be rejected before the key lookup.
		noCertFetch bool
	}{
		{
			name: "expired",
			token: func(t *testing.T) string {
				return sign(t, jwt.SigningMethodRS256, testSignKey, withClaim(validClaims(), "exp", time.Now().Add(-time.Minute).Unix()))
			},
		},
		{
			name: "not yet valid",
			token: func(t *testing.T) string {
				return sign(t, jwt.SigningMethodRS256, testSignKey, withClaim(validClaims(), "nbf", time.Now().Add(time.Hour).Unix()))
			},
		},
		{
			name: "missing subject",
			token: func(t *testing.T) string {
				return sign(t, jwt.SigningMethodRS256, testSignKey, withClaim(validClaims(), "sub", nil))
			},
		},
		{
			name:  "signed with another key",
			token: func(t *testing.T) string { return sign(t, jwt.SigningMethodRS256, testOtherKey, validClaims()) },
		},
		{
			name: "unknown kid",
			token: func(t *testing.T) string {
				token := jwt.NewWithClaims(jwt.SigningMethodRS256, validClaims())
				token.Header["kid"] = "other-kid"
				signed, err := token.SignedString(testSignKey)
				if err != nil {
					t.Fatal(err)
				}
				return signed
			},
		},
		{
			name: "alg none",
			token: func(t *testing.T) string {
				return sign(t, jwt.SigningMethodNone, jwt.UnsafeAllowNoneSignatureType, validClaims())
			},
			noCertFetch: true,
		},
		{
			name:        "HS256 keyed with the public key PEM",
			token:       func(t *testing.T) string { return sign(t, jwt.SigningMethodHS256, publicPEM, validClaims()) },
			noCertFetch: true,
		},
		{
			name:        "HS256 keyed with the public key DER",
			token:       func(t *testing.T) string { return sign(t, jwt.SigningMethodHS256, publicDER, validClaims()) },
			noCertFetch: true,
		},
		{
			name:        "ES256",
			token:       func(t *testing.T) string { return sign(t, jwt.SigningMethodES256, ecKey, validClaims()) },
			noCertFetch: true,
		},
		{
			name:  "malformed",
			token: func(t *testing.T) string { return "Bearer not.a.token" },
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			provider, hits := certServer(t)
			token, err := ParseWithValidation(provider, test.token(t))
			if !errors.Is(err, ErrInvalidAuth) {
				t.Fatalf("expected ErrInvalidAuth, got %v", err)
			}
			if token.Token != "" {
				t.Errorf("expected no Token on error, got %q", token.Token)
			}
			if test.noCertFetch && hits.Load() != 0 {
				t.Errorf("expected the alg to be rejected before the cert download, got %d downloads", hits.Load())
			}
		})
	}
}

func TestParseWithValidationMissingInput(t *testing.T) {
	provider, _ := certServer(t)
	if _, err := ParseWithValidation(provider, ""); !errors.Is(err, ErrMissingAuthToken) {
		t.Errorf("expected ErrMissingAuthToken, got %v", err)
	}
	if _, err := ParseWithValidation(nil, sign(t, jwt.SigningMethodRS256, testSignKey, validClaims())); err == nil {
		t.Error("expected an error without cert provider")
	}
}

// Parse does not verify, so it must keep accepting tokens that ParseWithValidation rejects.
func TestParseSkipsValidation(t *testing.T) {
	testKeys(t)
	tests := []struct {
		name  string
		token string
	}{
		{name: "expired", token: sign(t, jwt.SigningMethodRS256, testSignKey, withClaim(validClaims(), "exp", time.Now().Add(-time.Minute).Unix()))},
		{name: "signed with another key", token: sign(t, jwt.SigningMethodRS256, testOtherKey, validClaims())},
		{name: "HS256", token: sign(t, jwt.SigningMethodHS256, []byte("secret"), validClaims())},
		{name: "missing subject", token: sign(t, jwt.SigningMethodRS256, testSignKey, withClaim(validClaims(), "sub", nil))},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			raw := "Bearer " + test.token
			token, err := Parse(raw)
			if err != nil {
				t.Fatal(err)
			}
			if token.Token != raw || token.Username != "user1" {
				t.Errorf("unexpected claims %#v", token)
			}
		})
	}
}

func TestParseRejectsMalformedToken(t *testing.T) {
	if _, err := Parse(""); !errors.Is(err, ErrMissingAuthToken) {
		t.Errorf("expected ErrMissingAuthToken, got %v", err)
	}
	for _, raw := range []string{"Bearer abc", "Bearer a.b.c", "Bearer " + unsignedToken(`"not an object"`)} {
		token, err := Parse(raw)
		if !errors.Is(err, ErrInvalidAuth) {
			t.Errorf("%q: expected ErrInvalidAuth, got %v", raw, err)
		}
		if token.Token != "" {
			t.Errorf("%q: expected no Token on error, got %q", raw, token.Token)
		}
	}
}
