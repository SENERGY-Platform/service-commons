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
	"errors"
	"github.com/SENERGY-Platform/service-commons/pkg/cache/cacheerrors"
	"github.com/SENERGY-Platform/service-commons/pkg/cache/fallback"
	"github.com/SENERGY-Platform/service-commons/pkg/cache/interfaces"
	"github.com/SENERGY-Platform/service-commons/pkg/cache/localcache"
	"github.com/SENERGY-Platform/service-commons/pkg/signal"
	"log"
	"time"
)

func New(config Config) (cache *Cache, err error) {
	cache = &Cache{
		l1:            config.L1,
		l2:            config.L2,
		fallback:      config.Fallback,
		debug:         config.Debug,
		readCacheHook: config.ReadCacheHook,
		cacheMissHook: config.CacheMissHook,
	}
	if cache.l1 == nil && config.L1Provider != nil {
		cache.l1, err = config.L1Provider()
		if err != nil {
			return
		}
	}
	if cache.l1 == nil {
		cache.l1, err = localcache.New(60*time.Second, time.Second)
		if err != nil {
			return
		}
	}

	if cache.l2 == nil && config.L2Provider != nil {
		cache.l2, err = config.L2Provider()
		if err != nil {
			return
		}
	}

	if cache.fallback == nil && config.FallbackProvider != nil {
		cache.fallback, err = config.FallbackProvider()
		if err != nil {
			return
		}
	}

	err = cache.initCacheInvalidationHandler(config)
	if err != nil {
		return cache, err
	}
	return cache, nil
}

type Config struct {
	L1                            CacheImpl                 //optional, defaults to localcache.Cache (or L1Provider if provided) with 60s cache duration and 1s cleanup interval
	L1Provider                    func() (CacheImpl, error) //optional, may be used to create L1
	L2                            CacheImpl                 //optional
	L2Provider                    func() (CacheImpl, error) //optional, may be used to create L2
	Fallback                      *fallback.Fallback        //optional, only used in Use() and UseWithExpInGet() as a fallback to the get parameter
	FallbackProvider              func() (*fallback.Fallback, error)
	Debug                         bool
	ReadCacheHook                 func(duration time.Duration) //optional
	CacheMissHook                 func()                       //optional
	CacheInvalidationSignalHooks  map[Signal]ToKey
	CacheInvalidationSignalBroker *signal.Broker //optional, defaults to signal.DefaultBroker if CacheInvalidationSignalHooks is used
}

type Cache struct {
	l1            CacheImpl          //optional, defaults to localcache.Cache with 60s cache duration and 1s cleanup interval
	l2            CacheImpl          //optional
	fallback      *fallback.Fallback //optional, only used in Use() and UseWithExpInGet()
	debug         bool
	readCacheHook func(duration time.Duration) //optional
	cacheMissHook func()                       //optional
}

var ErrNotFound = cacheerrors.ErrNotFound

type CacheImpl = interfaces.CacheImpl

func (this *Cache) Get(key string) (item interface{}, err error) {
	start := time.Now()
	if this.readCacheHook != nil {
		defer this.readCacheHook(time.Since(start))
	}
	if this.cacheMissHook != nil {
		defer func() {
			if err != nil {
				this.cacheMissHook()
			}
		}()
	}

	item, err = this.l1.Get(key)
	if err == nil {
		return item, nil
	}
	if !errors.Is(err, ErrNotFound) {
		return item, err
	}
	if this.l2 == nil {
		return item, err
	}
	if this.debug {
		log.Println("DEBUG: use l2 cache", key, err)
	}
	var exp time.Duration
	item, exp, err = this.l2.GetWithExpiration(key)
	if err != nil {
		return item, err
	}
	if exp > 0 {
		_ = this.l1.Set(key, item, exp) // ignore l1 set err
	}
	return item, nil
}

func (this *Cache) Set(key string, value interface{}, exp time.Duration) (err error) {
	err = this.l1.Set(key, value, exp)
	if this.l2 != nil {
		err = errors.Join(err, this.l2.Set(key, value, exp))
	}
	return err
}

func (this *Cache) Remove(key string) (err error) {
	err = this.l1.Remove(key)
	if this.l2 != nil {
		err = errors.Join(err, this.l2.Remove(key))
	}
	return err
}

func (this *Cache) Reset() (err error) {
	err = this.l1.Reset()
	if this.l2 != nil {
		err = errors.Join(err, this.l2.Reset())
	}
	return err
}

func (this *Cache) Close() (err error) {
	err = this.l1.Close()
	if this.l2 != nil {
		err = errors.Join(err, this.l2.Close())
	}
	return err
}
