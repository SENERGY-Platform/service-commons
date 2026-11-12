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
	"fmt"
	"github.com/golang-jwt/jwt/v5"
	"net/http"
	"sync"
	"time"
)

// certFetchTimeout bounds one download of the Keycloak certs, so a slow Keycloak cannot hold requests.
const certFetchTimeout = 10 * time.Second

// minCertFetchInterval limits downloads triggered by unknown kids; a token with a made-up kid
// would otherwise cost one download each. A rotated key is found at most this long after the last download.
const minCertFetchInterval = 10 * time.Second

type KeycloakCertProvider struct {
	CertUrl string //https://{{keycloak}}/auth/realms/{{realm}}/protocol/openid-connect/certs

	certByKid map[string]*rsa.PublicKey
	mux       sync.RWMutex

	fetchMux  sync.Mutex
	lastFetch time.Time
	client    *http.Client
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
	// One download at a time; whoever waited finds the key in the cache or is throttled.
	this.fetchMux.Lock()
	defer this.fetchMux.Unlock()
	if key, ok := this.getCertByID(kid); ok {
		return key, nil
	}
	if !this.lastFetch.IsZero() && time.Since(this.lastFetch) < minCertFetchInterval {
		return nil, errors.New("unknown token kid")
	}
	this.lastFetch = time.Now()
	if this.client == nil {
		this.client = &http.Client{Timeout: certFetchTimeout}
	}
	resp, err := this.client.Get(this.CertUrl)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status %d from keycloak certs", resp.StatusCode)
	}
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
	// Reading a nil map is fine; creating it here would write under the read lock.
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
