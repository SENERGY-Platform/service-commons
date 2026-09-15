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
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestExchangeUserTokenV2Request(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		if r.URL.Path != "/auth/realms/master/protocol/openid-connect/token" {
			t.Errorf("unexpected path %s", r.URL.Path)
		}
		if r.Header.Get("Content-Type") != "application/x-www-form-urlencoded" {
			t.Errorf("unexpected content-type %s", r.Header.Get("Content-Type"))
		}
		if err := r.ParseForm(); err != nil {
			t.Fatal(err)
		}
		if got := r.PostForm.Get("grant_type"); got != "urn:ietf:params:oauth:grant-type:token-exchange" {
			t.Errorf("unexpected grant_type %q", got)
		}
		if got := r.PostForm.Get("subject_token"); got != "the-subject-token" {
			t.Errorf("unexpected subject_token %q", got)
		}
		if got := r.PostForm.Get("subject_token_type"); got != "urn:ietf:params:oauth:token-type:access_token" {
			t.Errorf("unexpected subject_token_type %q", got)
		}
		if got := r.PostForm.Get("requested_token_type"); got != "urn:ietf:params:oauth:token-type:access_token" {
			t.Errorf("unexpected requested_token_type %q", got)
		}
		if got := r.PostForm.Get("scope"); got != "profile email" {
			t.Errorf("unexpected scope %q", got)
		}
		if got := r.PostForm["audience"]; len(got) != 2 || got[0] != "client-a" || got[1] != "client-b" {
			t.Errorf("unexpected audience %v", got)
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(OpenidToken{
			AccessToken: unsignedToken(`{"sub":"u1"}`),
			ExpiresIn:   60,
		})
	}))
	defer server.Close()

	token, expiration, err := ExchangeUserTokenV2(server.URL, "the-subject-token",
		"urn:ietf:params:oauth:token-type:access_token", []string{"profile", "email"}, []string{"client-a", "client-b"})
	if err != nil {
		t.Fatal(err)
	}
	if token.Sub != "u1" {
		t.Errorf("unexpected subject %q", token.Sub)
	}
	if expiration.Seconds() != 55 {
		t.Errorf("expected expiration of 55s (60s minus 5s buffer), got %v", expiration)
	}
}

func TestExchangeUserTokenV2OmitsOptionalParams(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			t.Fatal(err)
		}
		if _, ok := r.PostForm["requested_token_type"]; ok {
			t.Error("requested_token_type should be omitted when empty")
		}
		if _, ok := r.PostForm["scope"]; ok {
			t.Error("scope should be omitted when empty")
		}
		if _, ok := r.PostForm["audience"]; ok {
			t.Error("audience should be omitted when empty")
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(OpenidToken{
			AccessToken: unsignedToken(`{"sub":"u1"}`),
			ExpiresIn:   60,
		})
	}))
	defer server.Close()

	_, _, err := ExchangeUserTokenV2(server.URL, "the-subject-token", "", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
}

func TestExchangeUserTokenV2Error(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))
	defer server.Close()

	_, _, err := ExchangeUserTokenV2(server.URL, "the-subject-token", "", nil, nil)
	if err == nil {
		t.Error("expected an error on a non-200 response")
	}
}
