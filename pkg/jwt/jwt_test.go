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

package jwt

import (
	"context"
	"github.com/SENERGY-Platform/service-commons/pkg/cache"
	"github.com/SENERGY-Platform/service-commons/pkg/cache/memcached"
	"github.com/SENERGY-Platform/service-commons/pkg/testing/docker"
	"sync"
	"testing"
	"time"
)

func TestWithCache(t *testing.T) {
	const testToken = "Bearer eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICIzaUtabW9aUHpsMmRtQnBJdS1vSkY4ZVVUZHh4OUFIckVOcG5CcHM5SjYwIn0.eyJqdGkiOiIzMmE1OTljZC0zNDgxLTQzYWUtYWY0NC04YTVmNjU4NzYxZTUiLCJleHAiOjE1NjI5MjAwMDUsIm5iZiI6MCwiaWF0IjoxNTYyOTE2NDA1LCJpc3MiOiJodHRwczovL2F1dGguc2VwbC5pbmZhaS5vcmcvYXV0aC9yZWFsbXMvbWFzdGVyIiwiYXVkIjoiZnJvbnRlbmQiLCJzdWIiOiJlYmJhZDkyNy00YzM5LTRkMTItODY5MC04OWIwNjdkZDRjZTciLCJ0eXAiOiJCZWFyZXIiLCJhenAiOiJmcm9udGVuZCIsIm5vbmNlIjoiNTVlMzA4N2UtZjljNi00MmQ2LWE0MmEtMGZiMjcxNWE4OTkyIiwiYXV0aF90aW1lIjoxNTYyOTE2NDA0LCJzZXNzaW9uX3N0YXRlIjoiYmU5MDQ2MmYtOGE3Yy00NWU4LTg1MjAtMGRlYzViZWI1ZWZlIiwiYWNyIjoiMSIsImFsbG93ZWQtb3JpZ2lucyI6WyIqIl0sInJlYWxtX2FjY2VzcyI6eyJyb2xlcyI6WyJ1bWFfYXV0aG9yaXphdGlvbiIsInVzZXIiXX0sInJlc291cmNlX2FjY2VzcyI6eyJhY2NvdW50Ijp7InJvbGVzIjpbIm1hbmFnZS1hY2NvdW50IiwibWFuYWdlLWFjY291bnQtbGlua3MiLCJ2aWV3LXByb2ZpbGUiXX19LCJyb2xlcyI6WyJ1bWFfYXV0aG9yaXphdGlvbiIsInVzZXIiLCJvZmZsaW5lX2FjY2VzcyJdLCJwcmVmZXJyZWRfdXNlcm5hbWUiOiJpbmdvIn0.pggKYb3V0VxFINWBqpFE_t14MKhSM7bhw8YqrYBRvOzh8ft7zu_-bOvLOYbJBwo0GU1D68U2d_eerkYEIt-mc0dNtdFasy5DG_GtvnWA4nsbf0BVsYKSZcRiDK4d4qbHu9NMjBdEwSkP9KDGEtou0yHtOnVzB1eHHNm_uSUO-O_kz2LWsXOPK2sbL1LTiCKS0XToJPdlaNczDMZB0nXR3sHbyi3Lwk-Va2ATS6Kke5M1KmFMowK-Y0jK2urt8GnCBIXvZMT6gUW9-dvlv4w_lAuVXQ9hFg_r0sBnoWzZOUR_xlrz2T-syjrZzmXlAkJrcD8KWPH-lCs0jD9pdiROhQ"

	c, err := cache.New(cache.Config{})
	if err != nil {
		t.Error(err)
		return
	}

	token, err := cache.Use(c, "token.1", func() (Token, error) {
		return Parse(testToken)
	}, cache.NoValidation[Token], time.Second)

	if token.Token == "" {
		t.Error("token is empty", token)
		return
	}
	t.Log(token)

	token, err = cache.Use(c, "token.1", func() (Token, error) {
		return Parse(testToken)
	}, cache.NoValidation[Token], time.Second)

	if token.Token == "" {
		t.Error("token is empty", token)
		return
	}
	t.Log(token)

	return
}

func TestWithMemcachedCache(t *testing.T) {
	const testToken = "Bearer eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICIzaUtabW9aUHpsMmRtQnBJdS1vSkY4ZVVUZHh4OUFIckVOcG5CcHM5SjYwIn0.eyJqdGkiOiIzMmE1OTljZC0zNDgxLTQzYWUtYWY0NC04YTVmNjU4NzYxZTUiLCJleHAiOjE1NjI5MjAwMDUsIm5iZiI6MCwiaWF0IjoxNTYyOTE2NDA1LCJpc3MiOiJodHRwczovL2F1dGguc2VwbC5pbmZhaS5vcmcvYXV0aC9yZWFsbXMvbWFzdGVyIiwiYXVkIjoiZnJvbnRlbmQiLCJzdWIiOiJlYmJhZDkyNy00YzM5LTRkMTItODY5MC04OWIwNjdkZDRjZTciLCJ0eXAiOiJCZWFyZXIiLCJhenAiOiJmcm9udGVuZCIsIm5vbmNlIjoiNTVlMzA4N2UtZjljNi00MmQ2LWE0MmEtMGZiMjcxNWE4OTkyIiwiYXV0aF90aW1lIjoxNTYyOTE2NDA0LCJzZXNzaW9uX3N0YXRlIjoiYmU5MDQ2MmYtOGE3Yy00NWU4LTg1MjAtMGRlYzViZWI1ZWZlIiwiYWNyIjoiMSIsImFsbG93ZWQtb3JpZ2lucyI6WyIqIl0sInJlYWxtX2FjY2VzcyI6eyJyb2xlcyI6WyJ1bWFfYXV0aG9yaXphdGlvbiIsInVzZXIiXX0sInJlc291cmNlX2FjY2VzcyI6eyJhY2NvdW50Ijp7InJvbGVzIjpbIm1hbmFnZS1hY2NvdW50IiwibWFuYWdlLWFjY291bnQtbGlua3MiLCJ2aWV3LXByb2ZpbGUiXX19LCJyb2xlcyI6WyJ1bWFfYXV0aG9yaXphdGlvbiIsInVzZXIiLCJvZmZsaW5lX2FjY2VzcyJdLCJwcmVmZXJyZWRfdXNlcm5hbWUiOiJpbmdvIn0.pggKYb3V0VxFINWBqpFE_t14MKhSM7bhw8YqrYBRvOzh8ft7zu_-bOvLOYbJBwo0GU1D68U2d_eerkYEIt-mc0dNtdFasy5DG_GtvnWA4nsbf0BVsYKSZcRiDK4d4qbHu9NMjBdEwSkP9KDGEtou0yHtOnVzB1eHHNm_uSUO-O_kz2LWsXOPK2sbL1LTiCKS0XToJPdlaNczDMZB0nXR3sHbyi3Lwk-Va2ATS6Kke5M1KmFMowK-Y0jK2urt8GnCBIXvZMT6gUW9-dvlv4w_lAuVXQ9hFg_r0sBnoWzZOUR_xlrz2T-syjrZzmXlAkJrcD8KWPH-lCs0jD9pdiROhQ"

	wg := &sync.WaitGroup{}
	defer wg.Wait()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, memcacheIp, err := docker.Memcached(ctx, wg)
	if err != nil {
		t.Error(err)
		return
	}

	l1, err := memcached.New(10, time.Minute, memcacheIp+":11211")
	if err != nil {
		t.Error(err)
		return
	}

	c, err := cache.New(cache.Config{L1: l1})
	if err != nil {
		t.Error(err)
		return
	}

	token, err := cache.Use(c, "token.1", func() (Token, error) {
		return Parse(testToken)
	}, cache.NoValidation[Token], time.Second)

	if token.Token == "" {
		t.Error("token is empty", token)
		return
	}
	t.Log(token)

	token, err = cache.Use(c, "token.1", func() (Token, error) {
		return Parse(testToken)
	}, cache.NoValidation[Token], time.Second)

	if token.Token == "" {
		t.Error("token is empty", token)
		return
	}
	t.Log(token)

	return
}
