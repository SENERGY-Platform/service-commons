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

package localcache

import (
	"github.com/SENERGY-Platform/service-commons/pkg/cache/cacheerrors"
	"github.com/SENERGY-Platform/service-commons/pkg/cache/interfaces"
	"github.com/patrickmn/go-cache"
	"time"
)

type Cache struct {
	l1 *cache.Cache
}

func New(defaultExp time.Duration, cleanupInterval time.Duration) (*Cache, error) {
	return &Cache{l1: cache.New(defaultExp, cleanupInterval)}, nil
}

func NewProvider(defaultExp time.Duration, cleanupInterval time.Duration) func() (interfaces.CacheImpl, error) {
	return func() (interfaces.CacheImpl, error) {
		return New(defaultExp, cleanupInterval)
	}
}

func (this *Cache) Get(key string) (value interface{}, resultType interfaces.ResultType, err error) {
	var found bool
	value, found = this.l1.Get(key)
	if !found {
		err = cacheerrors.ErrNotFound
	}
	return value, interfaces.ExactlyAsSet, err
}

func (this *Cache) GetWithExpiration(key string) (value interface{}, resultType interfaces.ResultType, exp time.Duration, err error) {
	var found bool
	var expTime time.Time
	value, expTime, found = this.l1.GetWithExpiration(key)
	if !found {
		err = cacheerrors.ErrNotFound
	}
	if err != nil {
		return value, interfaces.ExactlyAsSet, exp, err
	}
	exp = time.Until(expTime)
	return value, interfaces.ExactlyAsSet, exp, nil
}

func (this *Cache) Set(key string, value interface{}, exp time.Duration) (err error) {
	this.l1.Set(key, value, exp)
	return nil
}

func (this *Cache) Remove(key string) error {
	this.l1.Delete(key)
	return nil
}

func (this *Cache) Reset() error {
	this.l1.Flush()
	return nil
}

func (this *Cache) Close() (err error) {
	return nil
}
