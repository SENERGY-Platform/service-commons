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
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// KeycloakRealm is the realm ExchangeUserTokenV2 targets. Override it if the target realm is not "master".
var KeycloakRealm = "master"

// ExchangeUserTokenV2 implements Keycloak's "Standard token exchange", exchanging subjectToken
// for a new token. Parameters requestedTokenType, scope and audience are optional.
// See https://www.keycloak.org/securing-apps/token-exchange#_standard-token-exchange
func ExchangeUserTokenV2(keycloakEndpoint, subjectToken, requestedTokenType string, scope, audience []string) (token Token, expiration time.Duration, err error) {
	values := url.Values{
		"grant_type":         {"urn:ietf:params:oauth:grant-type:token-exchange"},
		"subject_token":      {subjectToken},
		"subject_token_type": {"urn:ietf:params:oauth:token-type:access_token"},
	}
	if requestedTokenType != "" {
		values.Set("requested_token_type", requestedTokenType)
	}
	if len(scope) > 0 {
		values.Set("scope", strings.Join(scope, " "))
	}
	for _, val := range audience {
		values.Add("audience", val)
	}
	resp, err := http.PostForm(keycloakEndpoint+"/auth/realms/"+KeycloakRealm+"/protocol/openid-connect/token", values)
	if err != nil {
		return
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		slog.Error("ExchangeUserTokenV2()", "status", resp.StatusCode, "error", string(body))
		err = errors.New("access denied")
		resp.Body.Close()
		return
	}
	var openIdToken OpenidToken
	err = json.NewDecoder(resp.Body).Decode(&openIdToken)
	if err != nil {
		return
	}
	token, err = Parse("Bearer " + openIdToken.AccessToken)
	return token, (time.Duration(openIdToken.ExpiresIn - 5)) * time.Second, err // subtract 5 seconds from expiration as a buffer
}
