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
	"crypto/rsa"
	"encoding/json"
	"errors"
	"github.com/golang-jwt/jwt"
	"net/http"
	"sync"
)

type KeycloakCertProvider struct {
	CertUrl string //https://{{keycloak}}/auth/realms/{{realm}}/protocol/openid-connect/certs

	certByKid map[string]*rsa.PublicKey
	mux       sync.RWMutex
}

type KeycloakCerts struct {
	Keys []struct {
		Kid     string   `json:"kid"`
		Kty     string   `json:"kty"`
		Alg     string   `json:"alg"`
		Use     string   `json:"use"`
		N       string   `json:"n"`
		E       string   `json:"e"`
		X5C     []string `json:"x5c"`
		X5T     string   `json:"x5t"`
		X5TS256 string   `json:"x5t#S256"`
	} `json:"keys"`
}

func (this *KeycloakCertProvider) GetKeycloakCert(token *jwt.Token) (interface{}, error) {
	kid, ok := token.Header["kid"].(string)
	if !ok {
		return nil, errors.New("missing token kid")
	}
	if key, ok := this.getCertByID(kid); ok {
		return key, nil
	}
	resp, err := http.Get(this.CertUrl)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	certs := KeycloakCerts{}
	err = json.NewDecoder(resp.Body).Decode(&certs)
	if err != nil {
		return nil, err
	}
	for _, cert := range certs.Keys {
		for _, k := range cert.X5C {
			pemPrefix := "-----BEGIN RSA PUBLIC KEY-----\n"
			pemSuffix := "\n-----END RSA PUBLIC KEY-----"
			key, err := jwt.ParseRSAPublicKeyFromPEM([]byte(pemPrefix + k + pemSuffix))
			if err != nil {
				return nil, err
			}
			this.setCertByID(cert.Kid, key)
		}
	}
	if key, ok := this.getCertByID(kid); ok {
		return key, nil
	}
	return nil, errors.New("no keycloak certs found")
}

func (this *KeycloakCertProvider) getCertByID(id string) (cert *rsa.PublicKey, ok bool) {
	this.mux.RLock()
	defer this.mux.RUnlock()
	if this.certByKid == nil {
		this.certByKid = map[string]*rsa.PublicKey{}
	}
	cert, ok = this.certByKid[id]
	return
}

func (this *KeycloakCertProvider) setCertByID(id string, cert *rsa.PublicKey) {
	this.mux.Lock()
	defer this.mux.Unlock()
	if this.certByKid == nil {
		this.certByKid = map[string]*rsa.PublicKey{}
	}
	this.certByKid[id] = cert
}
