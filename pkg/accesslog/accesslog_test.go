/*
 * Copyright 2024 InfAI (CC SES)
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

package accesslog

import (
	"encoding/json"
	"github.com/SENERGY-Platform/service-commons/pkg/jwt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func Test(t *testing.T) {
	calls := atomic.Int64{}
	calls.Store(0)
	callsWithToken := atomic.Int64{}
	callsWithToken.Store(0)

	server := httptest.NewServer(New(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		calls.Add(1)
		token, err := jwt.GetParsedToken(request)
		if err == nil && token.Token != "" {
			callsWithToken.Add(1)
			if _, ok := jwt.GetTokenFromContext(request.Context()); !ok {
				t.Error("missing token in context")
			}
		}
		if request.Method == http.MethodPost {
			msg, err := io.ReadAll(request.Body)
			if err != nil {
				t.Error(err)
			}
			if string(msg) != "testmessage" {
				t.Error(string(msg))
			}
		}

		if request.URL.Path == "/error" {
			http.Error(writer, "some error", 500)
			return
		}

		if request.URL.Path == "/panic" {
			panic("some panic")
		}

		if request.URL.Path == "/response" {
			json.NewEncoder(writer).Encode(map[string]interface{}{"foo": "bar"})
			return
		}

		if request.URL.Path == "/lock" {
			time.Sleep(15 * time.Second)
			json.NewEncoder(writer).Encode(map[string]interface{}{"foo": "bar"})
			return
		}

		writer.WriteHeader(200)
	})))
	defer server.Close()

	_, err := http.Get(server.URL + "/foo/bar?batz=42")
	if err != nil {
		t.Error(err)
		return
	}

	if c := calls.Load(); c != 1 {
		t.Error(c)
		return
	}
	if c := callsWithToken.Load(); c != 0 {
		t.Error(c)
		return
	}

	const testtoken = `Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJqdGkiOiIwOGM0N2E4OC0yYzc5LTQyMGYtODEwNC02NWJkOWViYmU0MWUiLCJleHAiOjE1NDY1MDcyMzMsIm5iZiI6MCwiaWF0IjoxNTQ2NTA3MTczLCJpc3MiOiJodHRwOi8vbG9jYWxob3N0OjgwMDEvYXV0aC9yZWFsbXMvbWFzdGVyIiwiYXVkIjoiZnJvbnRlbmQiLCJzdWIiOiJ0ZXN0T3duZXIiLCJ0eXAiOiJCZWFyZXIiLCJhenAiOiJmcm9udGVuZCIsIm5vbmNlIjoiOTJjNDNjOTUtNzViMC00NmNmLTgwYWUtNDVkZDk3M2I0YjdmIiwiYXV0aF90aW1lIjoxNTQ2NTA3MDA5LCJzZXNzaW9uX3N0YXRlIjoiNWRmOTI4ZjQtMDhmMC00ZWI5LTliNjAtM2EwYWUyMmVmYzczIiwiYWNyIjoiMCIsImFsbG93ZWQtb3JpZ2lucyI6WyIqIl0sInJlYWxtX2FjY2VzcyI6eyJyb2xlcyI6WyJ1c2VyIl19LCJyZXNvdXJjZV9hY2Nlc3MiOnsibWFzdGVyLXJlYWxtIjp7InJvbGVzIjpbInZpZXctcmVhbG0iLCJ2aWV3LWlkZW50aXR5LXByb3ZpZGVycyIsIm1hbmFnZS1pZGVudGl0eS1wcm92aWRlcnMiLCJpbXBlcnNvbmF0aW9uIiwiY3JlYXRlLWNsaWVudCIsIm1hbmFnZS11c2VycyIsInF1ZXJ5LXJlYWxtcyIsInZpZXctYXV0aG9yaXphdGlvbiIsInF1ZXJ5LWNsaWVudHMiLCJxdWVyeS11c2VycyIsIm1hbmFnZS1ldmVudHMiLCJtYW5hZ2UtcmVhbG0iLCJ2aWV3LWV2ZW50cyIsInZpZXctdXNlcnMiLCJ2aWV3LWNsaWVudHMiLCJtYW5hZ2UtYXV0aG9yaXphdGlvbiIsIm1hbmFnZS1jbGllbnRzIiwicXVlcnktZ3JvdXBzIl19LCJhY2NvdW50Ijp7InJvbGVzIjpbIm1hbmFnZS1hY2NvdW50IiwibWFuYWdlLWFjY291bnQtbGlua3MiLCJ2aWV3LXByb2ZpbGUiXX19LCJyb2xlcyI6WyJ1c2VyIl19.ykpuOmlpzj75ecSI6cHbCATIeY4qpyut2hMc1a67Ycg`

	req, err := http.NewRequest(http.MethodGet, server.URL+"/foo/bar?batz=42", nil)
	req.Header.Set("Authorization", testtoken)
	if err != nil {
		t.Error(err)
		return
	}

	_, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Error(err)
		return
	}

	if c := calls.Load(); c != 2 {
		t.Error(c)
	}
	if c := callsWithToken.Load(); c != 1 {
		t.Error(c)
		return
	}

	req, err = http.NewRequest(http.MethodPost, server.URL+"/foo/bar?batz=42", strings.NewReader("testmessage"))
	req.Header.Set("Authorization", testtoken)
	if err != nil {
		t.Error(err)
		return
	}

	_, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Error(err)
		return
	}

	if c := calls.Load(); c != 3 {
		t.Error(c)
	}
	if c := callsWithToken.Load(); c != 2 {
		t.Error(c)
		return
	}

	req, err = http.NewRequest(http.MethodGet, server.URL+"/error", nil)
	req.Header.Set("Authorization", testtoken)
	if err != nil {
		t.Error(err)
		return
	}

	_, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Error(err)
		return
	}

	if c := calls.Load(); c != 4 {
		t.Error(c)
	}
	if c := callsWithToken.Load(); c != 3 {
		t.Error(c)
		return
	}

	req, err = http.NewRequest(http.MethodGet, server.URL+"/response", nil)
	req.Header.Set("Authorization", testtoken)
	if err != nil {
		t.Error(err)
		return
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Error(err)
		return
	}

	if c := calls.Load(); c != 5 {
		t.Error(c)
	}
	if c := callsWithToken.Load(); c != 4 {
		t.Error(c)
		return
	}

	var body interface{}
	err = json.NewDecoder(resp.Body).Decode(&body)
	if err != nil {
		t.Error(err)
		return
	}
	if !reflect.DeepEqual(body, map[string]interface{}{"foo": "bar"}) {
		t.Error(body)
		return
	}

	req, err = http.NewRequest(http.MethodGet, server.URL+"/lock", nil)
	req.Header.Set("Authorization", testtoken)
	if err != nil {
		t.Error(err)
		return
	}

	_, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Error(err)
		return
	}

	if c := calls.Load(); c != 6 {
		t.Error(c)
	}
	if c := callsWithToken.Load(); c != 5 {
		t.Error(c)
		return
	}

	http.Get(server.URL + "/panic")
	if c := calls.Load(); c != 7 {
		t.Error(c)
	}
	if c := callsWithToken.Load(); c != 5 {
		t.Error(c)
		return
	}
}
