//go:build !ci
// +build !ci

/*
 * Copyright 2019 InfAI (CC SES)
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

package kafka

import (
	"context"
	"encoding/json"
	"github.com/SENERGY-Platform/service-commons/pkg/testing/docker"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestKafka(t *testing.T) {
	wg := &sync.WaitGroup{}
	defer wg.Wait()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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

	time.Sleep(2 * time.Second)

	consumed := []string{}
	mux := sync.Mutex{}

	wait := sync.WaitGroup{}

	kafkaConf := Config{
		KafkaUrl:      kafkaUrl,
		ConsumerGroup: "test",
		Debug:         true,
		TimeNow: func() time.Time {
			return time.Time{}
		},
	}

	err = NewConsumer(ctx, kafkaConf, "test", func(delivery []byte) error {
		mux.Lock()
		defer mux.Unlock()
		consumed = append(consumed, string(delivery))
		wait.Done()
		return nil
	})

	if err != nil {
		t.Error(err)
		return
	}

	producer, err := NewProducer(ctx, kafkaConf, "test")
	if err != nil {
		t.Error(err)
		return
	}

	wait.Add(1)
	err = producer.Produce("key", []byte("foo"))
	if err != nil {
		t.Error(err)
		return
	}

	wait.Add(1)
	err = producer.Produce("key", []byte("bar"))
	if err != nil {
		t.Error(err)
		return
	}

	wait.Wait()
	mux.Lock()
	defer mux.Unlock()

	if !reflect.DeepEqual(consumed, []string{"foo", "bar"}) {
		t.Error(consumed)
		return
	}
	//wait for finished commits
	time.Sleep(1 * time.Second)
}

func TestKafkaPartitions(t *testing.T) {
	wg := &sync.WaitGroup{}
	defer wg.Wait()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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

	kafkaConf := Config{
		KafkaUrl:      kafkaUrl,
		ConsumerGroup: "test",
		Debug:         true,
		TimeNow: func() time.Time {
			return time.Time{}
		},
	}

	time.Sleep(2 * time.Second)

	err = InitTopicWithPartitionNumber(kafkaUrl, 3, "test")
	if err != nil {
		t.Error(err)
		return
	}

	keys := []string{"a", "b", "c", "d", "e", "f", "g"}

	//key -> partition -> count
	consumed := map[string]map[string]int{}
	mux := sync.Mutex{}

	wait := sync.WaitGroup{}

	err = NewMultiConsumer(ctx, kafkaConf, []string{"test"}, func(delivery Message) error {
		mux.Lock()
		defer mux.Unlock()
		key := string(delivery.Key)
		partition := strconv.Itoa(delivery.Partition)
		if _, ok := consumed[key]; !ok {
			consumed[key] = map[string]int{}
		}
		if _, ok := consumed[key][partition]; !ok {
			consumed[key][partition] = 0
		}
		consumed[key][partition] = consumed[key][partition] + 1
		wait.Done()
		return nil
	})

	if err != nil {
		t.Error(err)
		return
	}

	producer, err := NewProducer(ctx, kafkaConf, "test")
	if err != nil {
		t.Error(err)
		return
	}

	for i := 0; i < 100; i++ {
		for _, key := range keys {
			wait.Add(1)
			err = producer.Produce(key, []byte("foo"))
			if err != nil {
				t.Error(err)
				return
			}
		}
	}

	wait.Wait()
	mux.Lock()
	defer mux.Unlock()

	temp, _ := json.Marshal(consumed)
	t.Log(string(temp))
	for key, m := range consumed {
		if len(m) > 1 {
			t.Error(key, m)
		}
	}

	//wait for finished commits
	time.Sleep(1 * time.Second)
}

func TestKafkaSubBalancer(t *testing.T) {
	wg := &sync.WaitGroup{}
	defer wg.Wait()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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

	kafkaConf := Config{
		KafkaUrl:      kafkaUrl,
		ConsumerGroup: "test",
		Debug:         true,
		TimeNow: func() time.Time {
			return time.Time{}
		},
	}

	time.Sleep(2 * time.Second)

	err = InitTopicWithPartitionNumber(kafkaUrl, 3, "test")
	if err != nil {
		t.Error(err)
		return
	}

	keys := []string{"foo/a", "foo/b", "bar/c", "foo/d", "bar/e", "bar/f", "foo/g"}

	//key -> partition -> count
	consumed := map[string]map[string]int{}
	mux := sync.Mutex{}

	wait := sync.WaitGroup{}

	err = NewMultiConsumer(ctx, kafkaConf, []string{"test"}, func(delivery Message) error {
		mux.Lock()
		defer mux.Unlock()
		key := string(delivery.Key)
		partition := strconv.Itoa(delivery.Partition)
		if _, ok := consumed[key]; !ok {
			consumed[key] = map[string]int{}
		}
		if _, ok := consumed[key][partition]; !ok {
			consumed[key][partition] = 0
		}
		consumed[key][partition] = consumed[key][partition] + 1
		wait.Done()
		return nil
	})

	if err != nil {
		t.Error(err)
		return
	}

	producer, err := NewProducerWithKeySeparationBalancer(ctx, kafkaConf, "test")
	if err != nil {
		t.Error(err)
		return
	}

	for i := 0; i < 100; i++ {
		for _, key := range keys {
			wait.Add(1)
			err = producer.Produce(key, []byte("foo"))
			if err != nil {
				t.Error(err)
				return
			}
		}
	}

	wait.Wait()
	mux.Lock()
	defer mux.Unlock()

	temp, _ := json.Marshal(consumed)
	t.Log(string(temp))
	for key, m := range consumed {
		if len(m) > 1 {
			t.Error(key, m)
		}
	}

	//wait for finished commits
	time.Sleep(1 * time.Second)
}

func TestKafkaLastOffset(t *testing.T) {
	wg := &sync.WaitGroup{}
	defer wg.Wait()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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

	producer, err := NewProducer(ctx, Config{
		KafkaUrl: kafkaUrl,
		Wg:       wg,
	}, "test")
	if err != nil {
		t.Error(err)
		return
	}

	err = producer.Produce("test", []byte("1"))
	if err != nil {
		t.Error(err)
		return
	}
	err = producer.Produce("test", []byte("2"))
	if err != nil {
		t.Error(err)
		return
	}
	time.Sleep(1 * time.Second)

	received := []string{}
	mux := sync.Mutex{}

	subCtx, subCancel := context.WithCancel(ctx)
	err = NewConsumer(subCtx, Config{
		KafkaUrl:      kafkaUrl,
		ConsumerGroup: "test",
		StartOffset:   LastOffset,
		Wg:            wg,
	}, "test", func(delivery []byte) error {
		mux.Lock()
		defer mux.Unlock()
		received = append(received, string(delivery))
		return nil
	})
	if err != nil {
		t.Error(err)
		return
	}

	time.Sleep(10 * time.Second)
	err = producer.Produce("test", []byte("3"))
	if err != nil {
		t.Error(err)
		return
	}
	err = producer.Produce("test", []byte("4"))
	if err != nil {
		t.Error(err)
		return
	}
	time.Sleep(10 * time.Second)
	subCancel()

	time.Sleep(10 * time.Second)

	err = producer.Produce("test", []byte("5"))
	if err != nil {
		t.Error(err)
		return
	}
	err = producer.Produce("test", []byte("6"))
	if err != nil {
		t.Error(err)
		return
	}
	time.Sleep(10 * time.Second)

	err = NewConsumer(ctx, Config{
		KafkaUrl:      kafkaUrl,
		ConsumerGroup: "test",
		StartOffset:   LastOffset,
		Wg:            wg,
	}, "test", func(delivery []byte) error {
		mux.Lock()
		defer mux.Unlock()
		received = append(received, string(delivery))
		return nil
	})
	if err != nil {
		t.Error(err)
		return
	}

	time.Sleep(10 * time.Second)

	err = producer.Produce("test", []byte("7"))
	if err != nil {
		t.Error(err)
		return
	}
	err = producer.Produce("test", []byte("8"))
	if err != nil {
		t.Error(err)
		return
	}
	time.Sleep(10 * time.Second)

	mux.Lock()
	defer mux.Unlock()

	if !reflect.DeepEqual(received, []string{"3", "4", "7", "8"}) {
		t.Error(received)
	}

}
