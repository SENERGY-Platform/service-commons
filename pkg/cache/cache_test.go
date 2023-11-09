/*
 * Copyright 2023 InfAI (CC SES)
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

package cache

import (
	"context"
	"errors"
	"github.com/SENERGY-Platform/service-commons/pkg/cache/fallback"
	"github.com/SENERGY-Platform/service-commons/pkg/cache/localcache"
	"github.com/SENERGY-Platform/service-commons/pkg/cache/memcached"
	"github.com/SENERGY-Platform/service-commons/pkg/signal"
	"github.com/SENERGY-Platform/service-commons/pkg/testing/docker"
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestMinimal(t *testing.T) {
	getterCalls := 0
	cache, err := New(Config{})
	if err != nil {
		t.Error(err)
		return
	}
	result, err := Use(cache, "foo", func() (string, error) {
		getterCalls = getterCalls + 1
		return "bar", nil
	}, time.Second)
	if err != nil {
		t.Error(err)
		return
	}
	if result != "bar" {
		t.Error(result)
		return
	}
	if getterCalls != 1 {
		t.Error(getterCalls)
		return
	}

	result, err = Use(cache, "foo", func() (string, error) {
		getterCalls = getterCalls + 1
		return "bar", nil
	}, time.Second)
	if err != nil {
		t.Error(err)
		return
	}
	if result != "bar" {
		t.Error(result)
		return
	}
	if getterCalls != 1 {
		t.Error(getterCalls)
		return
	}
}

func TestSignal(t *testing.T) {
	cache, err := New(Config{
		CacheInvalidationSignalHooks: map[Signal]ToKey{signal.Known.DeviceTypeCacheInvalidation: func(signalValue string) (cacheKey string) {
			return "dt." + signalValue
		}},
	})
	if err != nil {
		t.Error(err)
		return
	}

	getterIsCalled := false
	result, err := Use(cache, "dt.1", func() (string, error) {
		getterIsCalled = true
		return "bar", nil
	}, time.Second)
	if err != nil {
		t.Error(err)
		return
	}
	if result != "bar" {
		t.Error(result)
		return
	}
	if !getterIsCalled {
		t.Error(getterIsCalled)
		return
	}

	getterIsCalled = false
	result, err = Use(cache, "dt.2", func() (string, error) {
		getterIsCalled = true
		return "batz", nil
	}, time.Second)
	if err != nil {
		t.Error(err)
		return
	}
	if result != "batz" {
		t.Error(result)
		return
	}
	if !getterIsCalled {
		t.Error(getterIsCalled)
		return
	}

	getterIsCalled = false
	result, err = Use(cache, "dt.1", func() (string, error) {
		getterIsCalled = true
		return "bar2", nil
	}, time.Second)
	if err != nil {
		t.Error(err)
		return
	}
	if result != "bar" {
		t.Error(result)
		return
	}
	if getterIsCalled {
		t.Error(getterIsCalled)
		return
	}

	getterIsCalled = false
	result, err = Use(cache, "dt.2", func() (string, error) {
		getterIsCalled = true
		return "batz2", nil
	}, time.Second)
	if err != nil {
		t.Error(err)
		return
	}
	if result != "batz" {
		t.Error(result)
		return
	}
	if getterIsCalled {
		t.Error(getterIsCalled)
		return
	}

	signal.Known.DeviceTypeCacheInvalidation.Pub("1")

	time.Sleep(100 * time.Millisecond)

	getterIsCalled = false
	result, err = Use(cache, "dt.1", func() (string, error) {
		getterIsCalled = true
		return "bar3", nil
	}, time.Second)
	if err != nil {
		t.Error(err)
		return
	}
	if result != "bar3" {
		t.Error(result)
		return
	}
	if !getterIsCalled {
		t.Error(getterIsCalled)
		return
	}

	getterIsCalled = false
	result, err = Use(cache, "dt.2", func() (string, error) {
		getterIsCalled = true
		return "batz3", nil
	}, time.Second)
	if err != nil {
		t.Error(err)
		return
	}
	if result != "batz" {
		t.Error(result)
		return
	}
	if getterIsCalled {
		t.Error(getterIsCalled)
		return
	}

}

func TestExample(t *testing.T) {
	memcachUrls := []string{"example.url"}

	mux := sync.Mutex{}
	cacheReads := []time.Duration{}
	cacheMisses := 0

	cache, err := New(Config{
		L1Provider:       localcache.NewProvider(10*time.Minute, 50*time.Millisecond),
		L2Provider:       memcached.NewProvider(10, 10*time.Second, memcachUrls...),
		FallbackProvider: fallback.NewProvider(t.TempDir() + "/fallback.json"),
		Debug:            false,
		ReadCacheHook: func(duration time.Duration) {
			mux.Lock()
			defer mux.Unlock()
			cacheReads = append(cacheReads, duration)
		},
		CacheMissHook: func() {
			mux.Lock()
			defer mux.Unlock()
			cacheMisses = cacheMisses + 1
		},
		CacheInvalidationSignalHooks: map[Signal]ToKey{
			signal.Known.CacheInvalidationAll: nil,
			signal.Known.DeviceTypeCacheInvalidation: func(signalValue string) (cacheKey string) {
				return "dt." + signalValue
			},
		},
	})
	if err != nil {
		t.Error(err)
		return
	}

	defer func() {
		err = cache.Close()
		if err != nil {
			t.Error(err)
		}
	}()

}

func TestL2(t *testing.T) {
	l1, err := localcache.New(time.Hour, time.Minute)
	if err != nil {
		t.Error(err)
		return
	}
	l2, err := localcache.New(time.Hour, time.Minute)
	if err != nil {
		t.Error(err)
		return
	}

	cache, err := New(Config{L1: l1, L2: l2})
	if err != nil {
		t.Error(err)
		return
	}

	err = cache.Set("a", "a1", time.Minute)
	if err != nil {
		t.Error(err)
		return
	}

	if !checkCacheGet(t, cache, "a", "a1", nil) {
		return
	}
	if !checkCacheGet(t, l1, "a", "a1", nil) {
		return
	}
	if !checkCacheGet(t, l2, "a", "a1", nil) {
		return
	}

	err = l1.Set("a", "a2", time.Minute)
	if err != nil {
		t.Error(err)
		return
	}

	if !checkCacheGet(t, cache, "a", "a2", nil) {
		return
	}
	if !checkCacheGet(t, l1, "a", "a2", nil) {
		return
	}
	if !checkCacheGet(t, l2, "a", "a1", nil) {
		return
	}

	err = l1.Remove("a")
	if err != nil {
		t.Error(err)
		return
	}

	if !checkCacheGet(t, l1, "a", nil, ErrNotFound) {
		return
	}
	if !checkCacheGet(t, cache, "a", "a1", nil) {
		return
	}
	if !checkCacheGet(t, l1, "a", "a1", nil) {
		return
	}
	if !checkCacheGet(t, l2, "a", "a1", nil) {
		return
	}

	err = cache.Remove("a")
	if err != nil {
		t.Error(err)
		return
	}
	if !checkCacheGet(t, cache, "a", nil, ErrNotFound) {
		return
	}
	if !checkCacheGet(t, l1, "a", nil, ErrNotFound) {
		return
	}
	if !checkCacheGet(t, l2, "a", nil, ErrNotFound) {
		return
	}

}

func TestMemcachedL2(t *testing.T) {
	wg := &sync.WaitGroup{}
	defer wg.Wait()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, memcacheIp, err := docker.Memcached(ctx, wg)
	if err != nil {
		t.Error(err)
		return
	}

	l1, err := localcache.New(time.Hour, time.Minute)
	if err != nil {
		t.Error(err)
		return
	}
	l2, err := memcached.New(10, time.Minute, memcacheIp+":11211")
	if err != nil {
		t.Error(err)
		return
	}

	cache, err := New(Config{L1: l1, L2: l2})
	if err != nil {
		t.Error(err)
		return
	}

	err = cache.Set("a", "a1", time.Minute)
	if err != nil {
		t.Error(err)
		return
	}

	if !checkCacheGet(t, cache, "a", "a1", nil) {
		return
	}
	if !checkCacheGet(t, l1, "a", "a1", nil) {
		return
	}
	if !checkCacheGet(t, l2, "a", "a1", nil) {
		return
	}

	err = l1.Set("a", "a2", time.Minute)
	if err != nil {
		t.Error(err)
		return
	}

	if !checkCacheGet(t, cache, "a", "a2", nil) {
		return
	}
	if !checkCacheGet(t, l1, "a", "a2", nil) {
		return
	}
	if !checkCacheGet(t, l2, "a", "a1", nil) {
		return
	}

	err = l1.Remove("a")
	if err != nil {
		t.Error(err)
		return
	}

	if !checkCacheGet(t, l1, "a", nil, ErrNotFound) {
		return
	}
	if !checkCacheGet(t, cache, "a", "a1", nil) {
		return
	}
	if !checkCacheGet(t, l1, "a", "a1", nil) {
		return
	}
	if !checkCacheGet(t, l2, "a", "a1", nil) {
		return
	}

	err = cache.Remove("a")
	if err != nil {
		t.Error(err)
		return
	}
	if !checkCacheGet(t, cache, "a", nil, ErrNotFound) {
		return
	}
	if !checkCacheGet(t, l1, "a", nil, ErrNotFound) {
		return
	}
	if !checkCacheGet(t, l2, "a", nil, ErrNotFound) {
		return
	}

}

func TestUseFallback(t *testing.T) {
	cache, err := New(Config{FallbackProvider: fallback.NewProvider(t.TempDir() + "/fb.json")})
	if err != nil {
		t.Error(err)
		return
	}
	result, err := Use(cache, "test", func() (string, error) {
		return "foo", nil
	}, time.Minute)

	if err != nil || result != "foo" {
		t.Error(result, err)
		return
	}
	err = cache.Reset()
	if err != nil || result != "foo" {
		t.Error(result, err)
		return
	}

	if !checkCacheGet(t, cache, "test", nil, ErrNotFound) {
		return
	}

	result, err = Use(cache, "test", func() (string, error) {
		return "", errors.New("error")
	}, time.Minute)
	if err != nil || result != "foo" {
		t.Error(result, err)
		return
	}
}

func TestUseWithExpInGetFallback(t *testing.T) {
	cache, err := New(Config{FallbackProvider: fallback.NewProvider(t.TempDir() + "/fb.json")})
	if err != nil {
		t.Error(err)
		return
	}
	result, err := UseWithExpInGet(cache, "test", func() (string, time.Duration, error) {
		return "foo", time.Minute, nil
	}, time.Minute)

	if err != nil || result != "foo" {
		t.Error(result, err)
		return
	}
	err = cache.Reset()
	if err != nil || result != "foo" {
		t.Error(result, err)
		return
	}

	if !checkCacheGet(t, cache, "test", nil, ErrNotFound) {
		return
	}

	result, err = UseWithExpInGet(cache, "test", func() (string, time.Duration, error) {
		return "", time.Minute, errors.New("error")
	}, time.Minute)
	if err != nil || result != "foo" {
		t.Error(result, err)
		return
	}
}

func checkCacheGet(t *testing.T, cache interface {
	Get(string) (interface{}, error)
}, key string, expectedItem interface{}, expectedError error) bool {
	t.Helper()
	actualItem, actualErr := cache.Get(key)
	result := true
	if !reflect.DeepEqual(actualItem, expectedItem) {
		t.Errorf("unexpected item \n%#v\n!=\n%#v\n", actualItem, expectedItem)
		result = false
	}
	if !reflect.DeepEqual(actualErr, expectedError) {
		t.Errorf("unexpected err \n%#v\n!=\n%#v\n", actualErr, expectedError)
		result = false
	}
	return result
}
