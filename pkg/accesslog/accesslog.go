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
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"regexp"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	struct_logger "github.com/SENERGY-Platform/go-service-base/struct-logger"
	"github.com/SENERGY-Platform/service-commons/pkg/jwt"
)

var PreliminaryLogTimeout = 10 * time.Second

const (
	// callSourceLookupTimeout bounds the reverse lookup of the caller, which runs before the handler.
	// Without it a DNS server that does not answer PTR queries delays every request by the resolver's own timeout.
	callSourceLookupTimeout = 2 * time.Second
	callSourceNameTTL       = time.Hour       // IPs get reassigned, so a host name is looked up again after this
	callSourceFailureTTL    = 5 * time.Minute // failed lookups are cached too, so the timeout is not paid on every request
)

// sensitiveHeaders are request headers that may carry credentials; they are left out of the log (lower case).
// Sec-WebSocket-Protocol is included because browsers pass bearer tokens through it for websockets.
var sensitiveHeaders = map[string]struct{}{
	"authorization":          {},
	"proxy-authorization":    {},
	"cookie":                 {},
	"x-api-key":              {},
	"x-auth-token":           {},
	"sec-websocket-protocol": {},
}

func isSensitiveHeader(key string) bool {
	_, ok := sensitiveHeaders[strings.ToLower(key)]
	return ok
}

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

func New(handler http.Handler, options ...Options) http.Handler {
	loghandler := slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})
	logger := slog.New(loghandler)
	if len(options) > 0 && options[0].TrimFormat != "" {
		trimAttributes := []string{}
		if options[0].TrimAttributes != "" {
			trimAttributes = strings.Split(options[0].TrimAttributes, ",")
		}
		trimHandler, err := struct_logger.ApplyTrimFormat(loghandler, options[0].TrimFormat, trimAttributes)
		if err != nil {
			logger.Error("unable to apply trim format to logger -> use logger without trim", "error", err)
		} else {
			logger = slog.New(trimHandler)
		}
	}
	return NewWithLogger(handler, logger)
}

func NewWithLogger(handler http.Handler, logger *slog.Logger) http.Handler {
	var organization, project string
	if info, ok := debug.ReadBuildInfo(); ok {
		organization, project = getMeta(info.Main.Path)
	} else {
		project = info.Main.Path
	}
	return &AccessLogMiddleware{
		handler:     handler,
		resolver:    net.DefaultResolver,
		callSources: map[string]*callSource{},
		logger:      logger.With("organization", organization, "project", project, "log_record_type", "http-access"),
		errorlogger: logger.With("organization", organization, "project", project, "log_record_type", "http-access-panic")}
}

type Options struct {
	TrimFormat     string `json:"trim_format" env_var:"LOGGER_TRIM_FORMAT"`         //e.g. "20:[...]:10"; by default only used on the log message. if attributes should be trimmed, add them to TrimAttributes
	TrimAttributes string `json:"trim_attributes" env_var:"LOGGER_TRIM_ATTRIBUTES"` //comma seperated list of attribute keys that should also be trimmed
}

type AccessLogMiddleware struct {
	handler        http.Handler
	resolver       *net.Resolver
	callSources    map[string]*callSource // by remote IP; the port changes with every connection
	callSourcesMux sync.Mutex
	logger         *slog.Logger
	errorlogger    *slog.Logger
}

// callSource is the logged caller for one remote IP. name and expires are written once, before done is closed.
type callSource struct {
	name    string // host name, or the IP if the reverse lookup failed
	expires time.Time
	done    chan struct{}
}

func (this *callSource) resolved() bool {
	select {
	case <-this.done:
		return true
	default:
		return false
	}
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
		if !isSensitiveHeader(key) {
			headers[key] = request.Header.Get(key)
		}
	}

	body, err := io.ReadAll(request.Body)
	if err != nil {
		this.logger.Error("unable to read request body", "error", err)
	}
	modifiedRequest.Body = io.NopCloser(bytes.NewBuffer(body))
	finished := atomic.Bool{} // read by the preliminary log timer, written by finish
	args := []any{"method", method, "path", path, "caller", caller, "user", user, "headers", headers, "body", string(body)}
	time.AfterFunc(PreliminaryLogTimeout, func() {
		if !finished.Load() {
			finalArgs := append(args, "preliminary", true)
			this.logger.Info("", finalArgs...)
		}
	})
	return modifiedRequest, func(response *Response) {
		finished.Store(true)
		if r := recover(); r != nil {
			this.errorlogger.Error("recovered from panic", "error", fmt.Sprint(r), "stacktrace", string(debug.Stack()))
			http.Error(response, "Internal Server Error (recovered from panic)", 500)
		}
		finalArgs := append(args, "response-status-code", response.StatusCode, "request-duration-microseconds", time.Since(start).Microseconds())
		this.logger.Info("", finalArgs...)
	}
}

// getCallSource returns the host name of the caller, or its IP if the reverse lookup fails.
// Concurrent requests from the same IP wait for one shared lookup.
func (this *AccessLogMiddleware) getCallSource(req *http.Request) string {
	ip, _, err := net.SplitHostPort(req.RemoteAddr)
	if err != nil || ip == "" {
		return req.RemoteAddr
	}

	this.callSourcesMux.Lock()
	source, ok := this.callSources[ip]
	if ok && (!source.resolved() || time.Now().Before(source.expires)) {
		this.callSourcesMux.Unlock()
		<-source.done
		return source.name
	}
	source = &callSource{done: make(chan struct{})}
	this.callSources[ip] = source
	this.callSourcesMux.Unlock()

	source.name, source.expires = this.lookupCallSource(ip)
	close(source.done)
	return source.name
}

func (this *AccessLogMiddleware) lookupCallSource(ip string) (name string, expires time.Time) {
	// not bound to the request context: a client that disconnects early would otherwise cache a failure
	ctx, cancel := context.WithTimeout(context.Background(), callSourceLookupTimeout)
	defer cancel()
	names, _ := this.resolver.LookupAddr(ctx, ip)
	if len(names) == 0 {
		return ip, time.Now().Add(callSourceFailureTTL)
	}
	sort.Strings(names)
	return names[0], time.Now().Add(callSourceNameTTL)
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
