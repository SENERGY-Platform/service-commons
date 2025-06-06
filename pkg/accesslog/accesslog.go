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
	"fmt"
	"github.com/SENERGY-Platform/service-commons/pkg/jwt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"regexp"
	"runtime/debug"
	"sort"
	"sync"
	"time"
)

var PreliminaryLogTimeout = 10 * time.Second

//func New(handler http.Handler) http.Handler {
//	return NewWithLogger(handler, slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
//		Level: slog.LevelInfo,
//	})))
//}
//
//func NewWithLogger(handler http.Handler, logger *slog.Logger) http.Handler {
//	if info, ok := debug.ReadBuildInfo(); ok {
//		logger = logger.With("go-module", info.Path)
//	}
//	return &AccessLogMiddleware{
//		handler:            handler,
//		getCallSourceCache: map[string]string{},
//		logger:             logger.With("snrgy-log-type", "http-access"),
//		errorlogger:        logger.With("snrgy-log-type", "http-access-panic")}
//}

func New(handler http.Handler) http.Handler {
	return NewWithLogger(handler, slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))
}

func NewWithLogger(handler http.Handler, logger *slog.Logger) http.Handler {
	var organization, project string
	if info, ok := debug.ReadBuildInfo(); ok {
		organization, project = getMeta(info.Main.Path)
	} else {
		project = info.Main.Path
	}
	return &AccessLogMiddleware{
		handler:            handler,
		getCallSourceCache: map[string]string{},
		logger:             logger.With("organization", organization, "project", project, "log_record_type", "http-access"),
		errorlogger:        logger.With("organization", organization, "project", project, "log_record_type", "http-access-panic")}
}

type AccessLogMiddleware struct {
	handler               http.Handler
	getCallSourceCache    map[string]string
	getCallSourceCacheMux sync.Mutex
	logger                *slog.Logger
	errorlogger           *slog.Logger
}

func (this *AccessLogMiddleware) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	resp := NewResponse(w)
	modifiedRequest, finish := this.log(r)
	defer finish(resp)
	if this.handler != nil {
		this.handler.ServeHTTP(resp, modifiedRequest)
	} else {
		http.Error(w, "Forbidden", 403)
	}
}

func (this *AccessLogMiddleware) log(request *http.Request) (modifiedRequest *http.Request, finish func(*Response)) {
	start := time.Now()
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
	finished := false
	args := []any{"method", method, "path", path, "caller", caller, "user", user, "headers", headers, "body", string(body)}
	time.AfterFunc(PreliminaryLogTimeout, func() {
		if !finished {
			finalArgs := append(args, "preliminary", true)
			this.logger.Info("", finalArgs...)
		}
	})
	return modifiedRequest, func(response *Response) {
		finished = true
		if r := recover(); r != nil {
			this.errorlogger.Error("recovered from panic", "error", fmt.Sprint(r), "stacktrace", string(debug.Stack()))
			http.Error(response, "Internal Server Error (recovered from panic)", 500)
		}
		finalArgs := append(args, "response-status-code", response.StatusCode, "request-duration-microseconds", time.Since(start).Microseconds())
		this.logger.Info("", finalArgs...)
	}
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

var metaRegex = regexp.MustCompile(`(^[^/]+/[^/]+)/(.+$)`)

func getMeta(v string) (organization, project string) {
	if match := metaRegex.FindAllStringSubmatch(v, -1); len(match) > 0 {
		if len(match[0]) == 3 {
			organization = match[0][1]
			project = match[0][2]
		}
	}
	return
}
