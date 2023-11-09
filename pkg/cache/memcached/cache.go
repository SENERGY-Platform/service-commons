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

package memcached

import (
	"encoding/json"
	"errors"
	"github.com/SENERGY-Platform/service-commons/pkg/cache/cacheerrors"
	"github.com/SENERGY-Platform/service-commons/pkg/cache/interfaces"
	"github.com/bradfitz/gomemcache/memcache"
	"time"
)

func New(maxIdleConns int, timeout time.Duration, memcacheUrl ...string) (*Cache, error) {
	if len(memcacheUrl) == 0 {
		return nil, errors.New("missing memcached urls")
	}
	m := memcache.New(memcacheUrl...)
	m.MaxIdleConns = maxIdleConns
	m.Timeout = timeout
	m.Close()
	return &Cache{l1: m}, nil
}

func NewProvider(maxIdleConns int, timeout time.Duration, memcacheUrl ...string) func() (interfaces.CacheImpl, error) {
	return func() (interfaces.CacheImpl, error) {
		return New(maxIdleConns, timeout, memcacheUrl...)
	}
}

type Cache struct {
	l1 *memcache.Client
}

var ErrNotFound = cacheerrors.ErrNotFound

type Wrapper struct {
	ExpUnixDate int64       `json:"e"`
	Value       interface{} `json:"v"`
}

func (this *Cache) Get(key string) (value interface{}, err error) {
	var temp *memcache.Item
	temp, err = this.l1.Get(key)
	if err == memcache.ErrCacheMiss {
		err = ErrNotFound
		return value, err
	}
	if err != nil {
		return value, err
	}
	wrapper := Wrapper{}
	err = json.Unmarshal(temp.Value, &wrapper)
	if err != nil {
		return value, err
	}
	return wrapper.Value, nil
}

func (this *Cache) GetWithExpiration(key string) (value interface{}, exp time.Duration, err error) {
	var temp *memcache.Item
	temp, err = this.l1.Get(key)
	if errors.Is(err, memcache.ErrCacheMiss) {
		err = ErrNotFound
		return value, exp, err
	}
	if err != nil {
		return value, exp, err
	}
	wrapper := Wrapper{}
	err = json.Unmarshal(temp.Value, &wrapper)
	if err != nil {
		return value, exp, err
	}

	exp = time.Until(time.Unix(wrapper.ExpUnixDate, 0))

	return wrapper.Value, exp, nil
}

func (this *Cache) Set(key string, value interface{}, exp time.Duration) (err error) {
	item := &memcache.Item{Expiration: int32(exp.Seconds()), Key: key}
	item.Value, err = json.Marshal(Wrapper{
		ExpUnixDate: time.Now().Add(exp).Unix(),
		Value:       value,
	})
	if err != nil {
		return err
	}
	return this.l1.Set(item)
}

func (this *Cache) Remove(key string) error {
	return this.l1.Delete(key)
}

func (this *Cache) Reset() error {
	return this.l1.DeleteAll()
}

func (this *Cache) Close() (err error) {
	return this.l1.Close()
}
