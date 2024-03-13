/*
 * Copyright 2021 InfAI (CC SES)
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
	"errors"
	"fmt"
	"github.com/golang-jwt/jwt"
	"net/http"
	"strings"
)

var ErrInvalidAuth = errors.New("invalid auth token")
var ErrMissingAuthToken = errors.New("missing auth token")

func GetParsedToken(req *http.Request) (token Token, err error) {
	token, ok := GetTokenFromContext(req.Context())
	if ok {
		return token, nil
	}
	return Parse(GetAuthToken(req))
}

func GetAuthToken(req *http.Request) string {
	return req.Header.Get("Authorization")
}

func Parse(token string) (claims Token, err error) {
	if token == "" {
		return claims, ErrMissingAuthToken
	}
	orig := token
	if len(token) > 7 && strings.ToLower(token[:7]) == "bearer " {
		token = token[7:]
	}
	_, _, err = new(jwt.Parser).ParseUnverified(token, &claims)
	if err == nil {
		claims.Token = orig
	} else {
		err = fmt.Errorf("%w: %v", ErrInvalidAuth, err.Error())
	}
	return
}

type Token struct {
	Token       string              `json:"-"`
	Sub         string              `json:"sub,omitempty"`
	RealmAccess map[string][]string `json:"realm_access,omitempty"`
}

func (this *Token) String() string {
	return this.Token
}

func (this *Token) Jwt() string {
	return this.Token
}

func (this *Token) Valid() error {
	if this.Sub == "" {
		return errors.New("missing subject")
	}
	return nil
}

func (this *Token) IsAdmin() bool {
	return this.HasRole("admin")
}

func (this *Token) GetUserId() string {
	return this.Sub
}

func (this *Token) GetRoles() []string {
	return this.RealmAccess["roles"]
}

func (this *Token) HasRole(role string) bool {
	return contains(this.GetRoles(), role)
}

func contains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}
