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
	"github.com/SENERGY-Platform/service-commons/pkg/signal"
	"log"
)

type Signal = signal.Signal
type ToKey = func(signalValue string) (cacheKey string)

func (this *Cache) initCacheInvalidationHandler(config Config) (err error) {
	if config.CacheInvalidationSignalBroker == nil && len(config.CacheInvalidationSignalHooks) > 0 {
		config.CacheInvalidationSignalBroker = signal.DefaultBroker
	}
	for sig, valueToKey := range config.CacheInvalidationSignalHooks {
		sig := sig
		valueToKey := valueToKey
		if sig == signal.Known.CacheInvalidationAll || valueToKey == nil {
			config.CacheInvalidationSignalBroker.Sub("", sig, func(_ string) {
				if this.debug {
					log.Println("DEBUG: invalidate all cache values")
				}
				err = this.Reset()
				if err != nil {
					log.Println("ERROR: unable to invalidate cache", err)
				}
			})
		} else {
			config.CacheInvalidationSignalBroker.Sub("", sig, func(value string) {
				key := valueToKey(value)
				if this.debug {
					log.Println("DEBUG: invalidate cache", key)
				}
				err = this.Remove(key)
				if err != nil {
					log.Println("ERROR: unable to invalidate cache", err)
				}
			})
		}
	}
	return err
}
