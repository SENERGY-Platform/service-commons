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
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

func signWithKid(t *testing.T, kid string) string {
	t.Helper()
	token := jwt.NewWithClaims(jwt.SigningMethodRS256, validClaims())
	token.Header["kid"] = kid
	signed, err := token.SignedString(testSignKey)
	if err != nil {
		t.Fatal(err)
	}
	return signed
}

// Run with -race: the first lookups of a fresh provider happen concurrently.
func TestGetKeycloakCertConcurrentFirstUse(t *testing.T) {
	provider, _ := certServer(t)
	raw := sign(t, jwt.SigningMethodRS256, testSignKey, validClaims())
	wg := sync.WaitGroup{}
	errs := make(chan error, 20)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := ParseWithValidation(provider, raw); err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

func TestUnknownKidsDownloadAtMostOncePerInterval(t *testing.T) {
	provider, hits := certServer(t)
	for i := 0; i < 10; i++ {
		if _, err := ParseWithValidation(provider, signWithKid(t, fmt.Sprintf("made-up-%d", i))); err == nil {
			t.Fatal("expected a token with an unknown kid to be rejected")
		}
	}
	if hits.Load() != 1 {
		t.Errorf("expected 1 cert download for 10 unknown kids, got %d", hits.Load())
	}
	// The known key stays usable from the cache.
	if _, err := ParseWithValidation(provider, sign(t, jwt.SigningMethodRS256, testSignKey, validClaims())); err != nil {
		t.Errorf("valid token rejected after the throttled downloads: %v", err)
	}
	if hits.Load() != 1 {
		t.Errorf("expected the known kid to come from the cache, got %d downloads", hits.Load())
	}
}

func TestCertDownloadUsesTheProviderClient(t *testing.T) {
	testKeys(t)
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-time.After(5 * time.Second):
		}
	}))
	t.Cleanup(func() { close(release); server.Close() })
	provider := &KeycloakCertProvider{CertUrl: server.URL, client: &http.Client{Timeout: 100 * time.Millisecond}}
	start := time.Now()
	if _, err := ParseWithValidation(provider, sign(t, jwt.SigningMethodRS256, testSignKey, validClaims())); err == nil {
		t.Fatal("expected the stalled download to fail")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("expected the download to give up after the client timeout, took %v", elapsed)
	}
}

func TestDefaultClientHasATimeout(t *testing.T) {
	provider, _ := certServer(t)
	if _, err := ParseWithValidation(provider, sign(t, jwt.SigningMethodRS256, testSignKey, validClaims())); err != nil {
		t.Fatal(err)
	}
	if provider.client == nil || provider.client.Timeout != certFetchTimeout {
		t.Errorf("expected the default client to time out after %v", certFetchTimeout)
	}
}

func TestCertDownloadRejectsErrorStatus(t *testing.T) {
	testKeys(t)
	// A proxy error page that happens to carry valid certs must still not be trusted.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_ = json.NewEncoder(w).Encode(map[string]any{"keys": []map[string]any{{
			"kid": testKid, "x5c": []string{base64.StdEncoding.EncodeToString(testCertDER)},
		}}})
	}))
	t.Cleanup(server.Close)
	provider := &KeycloakCertProvider{CertUrl: server.URL}
	if _, err := provider.GetKeycloakCert(&jwt.Token{Header: map[string]any{"kid": testKid}}); err == nil {
		t.Fatal("expected a 502 from the certs endpoint to be an error")
	}
}
