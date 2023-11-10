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
	"github.com/SENERGY-Platform/service-commons/pkg/cache/invalidator"
	"github.com/SENERGY-Platform/service-commons/pkg/kafka"
	"github.com/SENERGY-Platform/service-commons/pkg/signal"
	"github.com/SENERGY-Platform/service-commons/pkg/testing/docker"
	"sync"
	"testing"
	"time"
)

func TestInvalidation(t *testing.T) {
	wg := &sync.WaitGroup{}
	defer wg.Wait()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	signal.DefaultBroker.Debug = true
	defer func() {
		signal.DefaultBroker.Debug = false
	}()

	_, zkIp, err := docker.Zookeeper(ctx, wg)
	if err != nil {
		t.Error(err)
		return
	}
	zkUrl := zkIp + ":2181"

	//kafka
	kafkaUrl, err := docker.Kafka(ctx, wg, zkUrl)
	if err != nil {
		t.Error(err)
		return
	}

	cache, err := New(Config{
		Debug: true,
		CacheInvalidationSignalHooks: map[Signal]ToKey{
			signal.Known.CacheInvalidationAll: nil,
			signal.Known.DeviceTypeCacheInvalidation: func(signalValue string) (cacheKey string) {
				return "dt." + signalValue
			},
			signal.Known.DeviceCacheInvalidation: func(signalValue string) (cacheKey string) {
				return "d." + signalValue
			},
		}})
	if err != nil {
		t.Error(err)
		return
	}

	err = invalidator.StartKnownCacheInvalidators(ctx, kafka.Config{
		KafkaUrl:      kafkaUrl,
		ConsumerGroup: "test",
		StartOffset:   kafka.LastOffset,
		Wg:            wg,
	}, invalidator.KnownTopics{
		DeviceTopic:     "devices",
		DeviceTypeTopic: "device-types",
	}, nil)
	if err != nil {
		t.Error(err)
		return
	}

	err = invalidator.StartCacheInvalidatorAll(ctx, kafka.Config{
		KafkaUrl:      kafkaUrl,
		ConsumerGroup: "test",
		StartOffset:   kafka.LastOffset,
		Wg:            wg,
	}, []string{"all"}, nil)
	if err != nil {
		t.Error(err)
		return
	}

	err = cache.Set("dt.1", "foo", time.Hour)
	if err != nil {
		t.Error(err)
		return
	}
	err = cache.Set("dt.2", "foo", time.Hour)
	if err != nil {
		t.Error(err)
		return
	}
	err = cache.Set("d.1", "foo", time.Hour)
	if err != nil {
		t.Error(err)
		return
	}
	err = cache.Set("d.2", "foo", time.Hour)
	if err != nil {
		t.Error(err)
		return
	}
	err = cache.Set("foo", "foo", time.Hour)
	if err != nil {
		t.Error(err)
		return
	}

	if !checkCacheGet(t, cache, "dt.1", "foo", nil) {
		return
	}
	if !checkCacheGet(t, cache, "dt.2", "foo", nil) {
		return
	}
	if !checkCacheGet(t, cache, "d.1", "foo", nil) {
		return
	}
	if !checkCacheGet(t, cache, "d.2", "foo", nil) {
		return
	}
	if !checkCacheGet(t, cache, "foo", "foo", nil) {
		return
	}

	time.Sleep(10 * time.Second)

	allProducer, err := kafka.NewProducer(ctx, kafka.Config{
		KafkaUrl: kafkaUrl,
		Wg:       wg,
	}, "all")
	if err != nil {
		t.Error(err)
		return
	}
	deviceTypeProducer, err := kafka.NewProducer(ctx, kafka.Config{
		KafkaUrl: kafkaUrl,
		Wg:       wg,
	}, "device-types")
	if err != nil {
		t.Error(err)
		return
	}
	deviceProducer, err := kafka.NewProducer(ctx, kafka.Config{
		KafkaUrl: kafkaUrl,
		Wg:       wg,
	}, "devices")
	if err != nil {
		t.Error(err)
		return
	}

	err = deviceProducer.Produce("test", []byte(`{"id":"1"}`))
	if err != nil {
		t.Error(err)
		return
	}
	err = deviceTypeProducer.Produce("test", []byte(`{"id":"2"}`))
	if err != nil {
		t.Error(err)
		return
	}

	time.Sleep(10 * time.Second)

	if !checkCacheGet(t, cache, "dt.1", "foo", nil) {
		return
	}
	if !checkCacheGet(t, cache, "dt.2", nil, ErrNotFound) {
		return
	}
	if !checkCacheGet(t, cache, "d.1", nil, ErrNotFound) {
		return
	}
	if !checkCacheGet(t, cache, "d.2", "foo", nil) {
		return
	}
	if !checkCacheGet(t, cache, "foo", "foo", nil) {
		return
	}

	err = allProducer.Produce("test", []byte(`foobar`))
	if err != nil {
		t.Error(err)
		return
	}

	time.Sleep(10 * time.Second)

	if !checkCacheGet(t, cache, "dt.1", nil, ErrNotFound) {
		return
	}
	if !checkCacheGet(t, cache, "dt.2", nil, ErrNotFound) {
		return
	}
	if !checkCacheGet(t, cache, "d.1", nil, ErrNotFound) {
		return
	}
	if !checkCacheGet(t, cache, "d.2", nil, ErrNotFound) {
		return
	}
	if !checkCacheGet(t, cache, "foo", nil, ErrNotFound) {
		return
	}

}

func TestInvalidationNoConsumerGroup(t *testing.T) {
	wg := &sync.WaitGroup{}
	defer wg.Wait()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	signal.DefaultBroker.Debug = true
	defer func() {
		signal.DefaultBroker.Debug = false
	}()

	_, zkIp, err := docker.Zookeeper(ctx, wg)
	if err != nil {
		t.Error(err)
		return
	}
	zkUrl := zkIp + ":2181"

	//kafka
	kafkaUrl, err := docker.Kafka(ctx, wg, zkUrl)
	if err != nil {
		t.Error(err)
		return
	}

	cache, err := New(Config{
		Debug: true,
		CacheInvalidationSignalHooks: map[Signal]ToKey{
			signal.Known.CacheInvalidationAll: nil,
			signal.Known.DeviceTypeCacheInvalidation: func(signalValue string) (cacheKey string) {
				return "dt." + signalValue
			},
			signal.Known.DeviceCacheInvalidation: func(signalValue string) (cacheKey string) {
				return "d." + signalValue
			},
		}})
	if err != nil {
		t.Error(err)
		return
	}

	err = invalidator.StartKnownCacheInvalidators(ctx, kafka.Config{
		KafkaUrl:    kafkaUrl,
		StartOffset: kafka.LastOffset,
		Wg:          wg,
	}, invalidator.KnownTopics{
		DeviceTopic:     "devices",
		DeviceTypeTopic: "device-types",
	}, nil)
	if err != nil {
		t.Error(err)
		return
	}

	err = invalidator.StartCacheInvalidatorAll(ctx, kafka.Config{
		KafkaUrl:    kafkaUrl,
		StartOffset: kafka.LastOffset,
		Wg:          wg,
	}, []string{"all"}, nil)
	if err != nil {
		t.Error(err)
		return
	}

	err = cache.Set("dt.1", "foo", time.Hour)
	if err != nil {
		t.Error(err)
		return
	}
	err = cache.Set("dt.2", "foo", time.Hour)
	if err != nil {
		t.Error(err)
		return
	}
	err = cache.Set("d.1", "foo", time.Hour)
	if err != nil {
		t.Error(err)
		return
	}
	err = cache.Set("d.2", "foo", time.Hour)
	if err != nil {
		t.Error(err)
		return
	}
	err = cache.Set("foo", "foo", time.Hour)
	if err != nil {
		t.Error(err)
		return
	}

	if !checkCacheGet(t, cache, "dt.1", "foo", nil) {
		return
	}
	if !checkCacheGet(t, cache, "dt.2", "foo", nil) {
		return
	}
	if !checkCacheGet(t, cache, "d.1", "foo", nil) {
		return
	}
	if !checkCacheGet(t, cache, "d.2", "foo", nil) {
		return
	}
	if !checkCacheGet(t, cache, "foo", "foo", nil) {
		return
	}

	time.Sleep(10 * time.Second)

	allProducer, err := kafka.NewProducer(ctx, kafka.Config{
		KafkaUrl: kafkaUrl,
		Wg:       wg,
	}, "all")
	if err != nil {
		t.Error(err)
		return
	}
	deviceTypeProducer, err := kafka.NewProducer(ctx, kafka.Config{
		KafkaUrl: kafkaUrl,
		Wg:       wg,
	}, "device-types")
	if err != nil {
		t.Error(err)
		return
	}
	deviceProducer, err := kafka.NewProducer(ctx, kafka.Config{
		KafkaUrl: kafkaUrl,
		Wg:       wg,
	}, "devices")
	if err != nil {
		t.Error(err)
		return
	}

	err = deviceProducer.Produce("test", []byte(`{"id":"1"}`))
	if err != nil {
		t.Error(err)
		return
	}
	err = deviceTypeProducer.Produce("test", []byte(`{"id":"2"}`))
	if err != nil {
		t.Error(err)
		return
	}

	time.Sleep(10 * time.Second)

	if !checkCacheGet(t, cache, "dt.1", "foo", nil) {
		return
	}
	if !checkCacheGet(t, cache, "dt.2", nil, ErrNotFound) {
		return
	}
	if !checkCacheGet(t, cache, "d.1", nil, ErrNotFound) {
		return
	}
	if !checkCacheGet(t, cache, "d.2", "foo", nil) {
		return
	}
	if !checkCacheGet(t, cache, "foo", "foo", nil) {
		return
	}

	err = allProducer.Produce("test", []byte(`foobar`))
	if err != nil {
		t.Error(err)
		return
	}

	time.Sleep(10 * time.Second)

	if !checkCacheGet(t, cache, "dt.1", nil, ErrNotFound) {
		return
	}
	if !checkCacheGet(t, cache, "dt.2", nil, ErrNotFound) {
		return
	}
	if !checkCacheGet(t, cache, "d.1", nil, ErrNotFound) {
		return
	}
	if !checkCacheGet(t, cache, "d.2", nil, ErrNotFound) {
		return
	}
	if !checkCacheGet(t, cache, "foo", nil, ErrNotFound) {
		return
	}

}
