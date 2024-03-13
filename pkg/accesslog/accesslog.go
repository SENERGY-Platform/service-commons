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
	"errors"
	"github.com/SENERGY-Platform/service-commons/pkg/jwt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"runtime/debug"
	"sort"
	"sync"
)

func New(handler http.Handler) http.Handler {
	return NewWithLogger(handler, slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))
}

func NewWithLogger(handler http.Handler, logger *slog.Logger) http.Handler {
	if info, ok := debug.ReadBuildInfo(); ok {
		logger = logger.With("snrgy-log-type", "http-access", "go-module", info.Path)
	}
	return &AccessLogMiddleware{handler: handler, getCallSourceCache: map[string]string{}, logger: logger}
}

type AccessLogMiddleware struct {
	handler               http.Handler
	getCallSourceCache    map[string]string
	getCallSourceCacheMux sync.Mutex
	logger                *slog.Logger
}

func (this *AccessLogMiddleware) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	modifiedRequest := this.log(r)
	if this.handler != nil {
		this.handler.ServeHTTP(w, modifiedRequest)
	} else {
		http.Error(w, "Forbidden", 403)
	}
}

func (this *AccessLogMiddleware) log(request *http.Request) (modifiedRequest *http.Request) {
	method := request.Method
	path := request.URL.String()
	caller := this.getCallSource(request)

	modifiedRequest = request

	user := ""
	token, err := jwt.GetParsedToken(request)
	if err == nil {
		user = token.GetUserId()
		modifiedRequest = request.WithContext(jwt.AddTokenToContext(request.Context(), token))
	} else if !errors.Is(err, jwt.ErrMissingAuthToken) {
		this.logger.Warn("unable to parse auth header", "error", err)
	}

	headers := map[string]string{}
	for key, _ := range request.Header {
		if key != "Authorization" {
			headers[key] = request.Header.Get(key)
		}
	}

	body, err := io.ReadAll(request.Body)
	if err != nil {
		this.logger.Error("unable to read request body", "error", err)
	}
	modifiedRequest.Body = io.NopCloser(bytes.NewBuffer(body))

	this.logger.Info("", "method", method, "path", path, "caller", caller, "user", user, "headers", headers, "body", string(body))
	return modifiedRequest
}

func (this *AccessLogMiddleware) getCallSource(req *http.Request) (result string) {
	this.getCallSourceCacheMux.Lock()
	var cacheHit bool
	result, cacheHit = this.getCallSourceCache[req.RemoteAddr]
	this.getCallSourceCacheMux.Unlock()
	if cacheHit {
		return result
	}

	result = req.RemoteAddr
	remoteAddr, _, err := net.SplitHostPort(result)
	if err == nil && remoteAddr != "" {
		remoteHosts, _ := net.LookupAddr(remoteAddr)
		if len(remoteHosts) > 0 {
			sort.Strings(remoteHosts)
			result = remoteHosts[0]
			this.getCallSourceCacheMux.Lock()
			this.getCallSourceCache[req.RemoteAddr] = result
			this.getCallSourceCacheMux.Unlock()
		}
	}

	return result
}
