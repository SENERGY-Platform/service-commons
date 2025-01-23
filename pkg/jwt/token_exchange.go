/*
 * Copyright 2025 InfAI (CC SES)
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
	"log"
	"net/http"
	"net/url"
	"time"
)

func exchangeUserToken(keycloakEndpoint, clientId, clientSecret, userId string) (token Token, expiration time.Duration, err error) {
	resp, err := http.PostForm(keycloakEndpoint+"/auth/realms/master/protocol/openid-connect/token", url.Values{
		"client_id":         {clientId},
		"client_secret":     {clientSecret},
		"grant_type":        {"urn:ietf:params:oauth:grant-type:token-exchange"},
		"requested_subject": {userId},
	})
	if err != nil {
		return
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		log.Println("ERROR: GetUserToken()", resp.StatusCode, string(body))
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

type OpenidToken struct {
	AccessToken      string    `json:"access_token"`
	ExpiresIn        float64   `json:"expires_in"`
	RefreshExpiresIn float64   `json:"refresh_expires_in"`
	RefreshToken     string    `json:"refresh_token"`
	TokenType        string    `json:"token_type"`
	RequestTime      time.Time `json:"-"`
	ParsedToken      Token     `json:"-"`
}
