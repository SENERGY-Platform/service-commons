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
	"fmt"
	"log"
	"time"

	"github.com/SENERGY-Platform/service-commons/pkg/cache/fallback"
)

// UseWithAsyncRefresh returns value from cache/fallback and later tries to update cache from source
func UseWithAsyncRefresh[T any](cache *Cache, key string, get func() (T, error), validate func(T) error, exp time.Duration, l2Exp ...time.Duration) (result T, err error) {
	if cache == nil {
		return get()
	}
	var temp interface{}
	var ok bool
	temp, err = Get[T](cache, key, validate)
	if err == nil {
		result, ok = temp.(T)
		if ok {
			return result, nil
		} else {
			log.Printf("WARNING: cached value is of unexpected type: got %#v, want %#v", temp, result)
		}
	}
	go func() {
		//try cache refresh but use the fallback file in the meantime
		result, err = get()
		if err != nil {
			log.Println("WARNING: unable to refresh cache value", err)
			return
		}
		cache.Set(key, result, exp, l2Exp...)
		if cache.fallback != nil {
			cache.fallback.Set(key, result)
		}
	}()
	if cache.fallback != nil {
		temp, err = fallback.Get[T](cache.fallback, key)
		if err != nil {
			return result, err
		}
		result, ok = temp.(T)
		if !ok {
			err = fmt.Errorf("WARNING: fallback value is of unexpected type: got %#v, want %#v", temp, result)
		}
		return result, err
	}
	return get()
}

// Use tries to retrieve a key from the Cache and cast the result as T
// if unsuccessful (because a cache-miss or a validation error) the cache is updated wit a value received from the 'get' function parameter
// the 'exp' and 'l2Exp' parameters are passed to the Cache.Set method and define the expiration time of newly cached values
//   - 'exp' defines the time until the value is expired and cant be retrieved
//   - 'l2Exp' (optional) is used to define a separate expiration date for the l2 cache (only used if Cache.l2 is set, only first value is used,if no l2Exp is set, the exp parameter is used)
//   - the 'validate' function parameter is passed to the Get function to prevent poisoned or stale cache values; if no validation is needed 'NoValidation[T]' or 'nil' may be used
func Use[T any](cache *Cache, key string, get func() (T, error), validate func(T) error, exp time.Duration, l2Exp ...time.Duration) (result T, err error) {
	if cache == nil {
		return get()
	}
	var temp interface{}
	var ok bool
	temp, err = Get[T](cache, key, validate)
	if err == nil {
		result, ok = temp.(T)
		if ok {
			return result, nil
		} else {
			log.Printf("WARNING: cached value is of unexpected type: got %#v, want %#v", temp, result)
		}
	}
	result, err = get()
	if err != nil {
		if cache.fallback != nil {
			temp, err = fallback.Get[T](cache.fallback, key)
			if err != nil {
				return result, err
			}
			result, ok = temp.(T)
			if !ok {
				err = fmt.Errorf("WARNING: fallback value is of unexpected type: got %#v, want %#v", temp, result)
			}
		}
		if err != nil {
			return result, err
		}
	} else {
		if cache.fallback != nil {
			err = cache.fallback.Set(key, result)
		}
		if err != nil {
			log.Println("WARNING: unable to store value in fallback storage", err)
			err = nil
		}
	}
	_ = cache.Set(key, result, exp, l2Exp...) // ignore set errors
	return result, nil
}

// UseWithExpInGet tries to retrieve a key from the Cache.
// if unsuccessful (because of a cache-miss or a validation error) the cache is updated wit a value received from the 'get' function parameter.
// the 'validate' function parameter is passed to the Get function to prevent poisoned or stale cache values.
func UseWithExpInGet[T any](cache *Cache, key string, get func() (T, time.Duration, error), validate func(T) error, fallbackExp time.Duration) (result T, err error) {
	if cache == nil {
		result, _, err = get()
		return result, err
	}
	var temp interface{}
	var ok bool
	temp, err = Get[T](cache, key, validate)
	if err == nil {
		result, ok = temp.(T)
		if ok {
			return result, nil
		} else {
			log.Printf("WARNING: cached value is of unexpected type: got %#v, want %#v", temp, result)
		}
	}
	var exp time.Duration
	result, exp, err = get()
	if err != nil {
		if cache.fallback != nil {
			temp, err = fallback.Get[T](cache.fallback, key)
			if err != nil {
				return result, err
			}
			result, ok = temp.(T)
			if !ok {
				err = fmt.Errorf("WARNING: fallback value is of unexpected type: got %#v, want %#v", temp, result)
			}
		}
		if err != nil {
			return result, err
		}
	} else {
		if cache.fallback != nil {
			err = cache.fallback.Set(key, result)
		}
		if err != nil {
			log.Println("WARNING: unable to store value in fallback storage", err)
			err = nil
		}
	}
	if exp <= 0 {
		exp = fallbackExp
	}
	_ = cache.Set(key, result, exp) // ignore set errors
	return result, nil
}

func NoValidation[T any](T) error {
	return nil
}
