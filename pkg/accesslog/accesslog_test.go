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
	"bytes"
	"context"
	"encoding/json"
	"github.com/SENERGY-Platform/service-commons/pkg/jwt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"sync"
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

func TestSensitiveHeadersAreNotLogged(t *testing.T) {
	buf := &syncBuffer{}
	logger := slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelInfo}))

	blocked := map[string]string{
		"Authorization":          "Bearer secret-authorization-value",
		"Proxy-Authorization":    "Basic secret-proxy-authorization-value",
		"Cookie":                 "session=secret-cookie-value",
		"X-Api-Key":              "secret-api-key-value",
		"X-Auth-Token":           "secret-auth-token-value",
		"Sec-WebSocket-Protocol": "secret-websocket-protocol-value",
	}

	server := httptest.NewServer(NewWithLogger(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}), logger))
	defer server.Close()

	req, err := http.NewRequest(http.MethodGet, server.URL+"/foo", nil)
	if err != nil {
		t.Fatal(err)
	}
	for key, value := range blocked {
		req.Header.Set(key, value)
	}
	req.Header.Set("X-Custom-Header", "normal-header-value")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	// the access log line is written after the handler returned, so it may arrive after the response
	deadline := time.Now().Add(5 * time.Second)
	for !strings.Contains(buf.String(), "response-status-code") && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}

	out := buf.String()
	if !strings.Contains(out, "normal-header-value") {
		t.Errorf("normal header missing from log: %s", out)
	}
	for key, value := range blocked {
		if strings.Contains(out, value) {
			t.Errorf("value of header %s found in log", key)
		}
	}
}

// TestUnansweredReverseLookup uses a DNS server that never answers, like the embedded Docker DNS for PTR queries
// of container IPs. With the resolver defaults (5 s, 2 attempts) a request used to wait 10 s.
func TestUnansweredReverseLookup(t *testing.T) {
	dns, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer dns.Close()
	queries := atomic.Int64{}
	go func() {
		packet := make([]byte, 512)
		for {
			if _, _, err := dns.ReadFrom(packet); err != nil {
				return
			}
			queries.Add(1)
		}
	}()

	buf := &syncBuffer{}
	handler := NewWithLogger(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}), slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelInfo})))
	middleware := handler.(*AccessLogMiddleware)
	middleware.resolver = &net.Resolver{
		PreferGo: true,
		Dial: func(ctx context.Context, _, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "udp", dns.LocalAddr().String())
		},
	}

	// documentation addresses (RFC 5737), not in /etc/hosts, so the lookup has to ask the DNS server
	serve := func(remoteAddr string) time.Duration {
		req := httptest.NewRequest(http.MethodGet, "/foo", nil)
		req.RemoteAddr = remoteAddr
		start := time.Now()
		handler.ServeHTTP(httptest.NewRecorder(), req)
		return time.Since(start)
	}

	if d := serve("192.0.2.1:40001"); d > 3*time.Second {
		t.Fatalf("request with unanswered reverse lookup took %v", d)
	}
	if !strings.Contains(buf.String(), `"caller":"192.0.2.1"`) {
		t.Fatalf("caller is not the IP: %s", buf.String())
	}
	queriesPerLookup := queries.Load()
	if queriesPerLookup == 0 {
		t.Fatal("reverse lookup did not reach the test DNS server")
	}

	middleware.callSourcesMux.Lock()
	ttl := time.Until(middleware.callSources["192.0.2.1"].expires)
	middleware.callSourcesMux.Unlock()
	if ttl <= callSourceFailureTTL-time.Minute || ttl > callSourceFailureTTL {
		t.Fatalf("failed lookup cached for %v, want %v", ttl, callSourceFailureTTL)
	}

	// a new connection from the same IP comes from a new port and must hit the cache
	if d := serve("192.0.2.1:40002"); d > 500*time.Millisecond {
		t.Errorf("cached request took %v", d)
	}
	if q := queries.Load(); q != queriesPerLookup {
		t.Errorf("cached request sent %d DNS queries", q-queriesPerLookup)
	}

	// concurrent requests from a new IP share one lookup
	wg := sync.WaitGroup{}
	for port := 40001; port <= 40010; port++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			serve(net.JoinHostPort("192.0.2.2", strconv.Itoa(port)))
		}()
	}
	wg.Wait()
	if q := queries.Load() - queriesPerLookup; q != queriesPerLookup {
		t.Errorf("10 concurrent requests sent %d DNS queries, one lookup sends %d", q, queriesPerLookup)
	}

	// an expired entry is looked up again
	middleware.callSourcesMux.Lock()
	middleware.callSources["192.0.2.1"].expires = time.Now().Add(-time.Second)
	middleware.callSourcesMux.Unlock()
	before := queries.Load()
	serve("192.0.2.1:40003")
	if queries.Load() == before {
		t.Error("expired entry was not looked up again")
	}
}

// TestAnsweredReverseLookup resolves 8.8.8.8 through the public resolver 1.1.1.1, which answers PTR queries.
func TestAnsweredReverseLookup(t *testing.T) {
	dials := atomic.Int64{}
	resolver := &net.Resolver{
		PreferGo: true,
		Dial: func(ctx context.Context, network, _ string) (net.Conn, error) {
			dials.Add(1)
			var d net.Dialer
			return d.DialContext(ctx, network, "1.1.1.1:53")
		},
	}
	probeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := resolver.LookupAddr(probeCtx, "8.8.8.8"); err != nil {
		t.Skipf("public resolver 1.1.1.1 not reachable: %v", err)
	}

	buf := &syncBuffer{}
	handler := NewWithLogger(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}), slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelInfo})))
	middleware := handler.(*AccessLogMiddleware)
	middleware.resolver = resolver

	serve := func(remoteAddr string) {
		req := httptest.NewRequest(http.MethodGet, "/foo", nil)
		req.RemoteAddr = remoteAddr
		handler.ServeHTTP(httptest.NewRecorder(), req)
	}

	dialsBefore := dials.Load()
	serve("8.8.8.8:40001")
	if !strings.Contains(buf.String(), `"caller":"dns.google."`) {
		t.Fatalf("caller is not the host name: %s", buf.String())
	}

	middleware.callSourcesMux.Lock()
	ttl := time.Until(middleware.callSources["8.8.8.8"].expires)
	middleware.callSourcesMux.Unlock()
	if ttl <= callSourceNameTTL-time.Minute || ttl > callSourceNameTTL {
		t.Fatalf("host name cached for %v, want %v", ttl, callSourceNameTTL)
	}

	dialsAfterLookup := dials.Load()
	if dialsAfterLookup == dialsBefore {
		t.Fatal("reverse lookup did not use the test resolver")
	}
	serve("8.8.8.8:40002")
	if dials.Load() != dialsAfterLookup {
		t.Error("cached host name was looked up again")
	}
	if n := strings.Count(buf.String(), `"caller":"dns.google."`); n != 2 {
		t.Errorf("host name logged %d times, want 2", n)
	}
}

type syncBuffer struct {
	mux sync.Mutex
	buf bytes.Buffer
}

func (this *syncBuffer) Write(p []byte) (int, error) {
	this.mux.Lock()
	defer this.mux.Unlock()
	return this.buf.Write(p)
}

func (this *syncBuffer) String() string {
	this.mux.Lock()
	defer this.mux.Unlock()
	return this.buf.String()
}
