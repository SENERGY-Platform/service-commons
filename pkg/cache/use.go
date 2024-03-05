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
	"github.com/SENERGY-Platform/service-commons/pkg/cache/fallback"
	"log"
	"time"
)

func Use[T any](cache *Cache, key string, get func() (T, error), exp time.Duration) (result T, err error) {
	if cache == nil {
		return get()
	}
	var temp interface{}
	var ok bool
	temp, err = Get[T](cache, key)
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
	_ = cache.Set(key, result, exp) // ignore set errors
	return result, nil
}

func UseWithExpInGet[T any](cache *Cache, key string, get func() (T, time.Duration, error), fallbackExp time.Duration) (result T, err error) {
	if cache == nil {
		result, _, err = get()
		return result, err
	}
	var temp interface{}
	var ok bool
	temp, err = Get[T](cache, key)
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
