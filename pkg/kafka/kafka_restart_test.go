/*
 * Copyright (c) 2026 InfAI (CC SES)
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
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/SENERGY-Platform/service-commons/pkg/testing/docker"
)

// a listener error used to end the consumer goroutine, which left the process
// running without a consumer until someone restarted the pod by hand
func TestKafkaGroupConsumerSurvivesListenerError(t *testing.T) {
	wg := &sync.WaitGroup{}
	defer wg.Wait()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	kafkaUrl, err := docker.Kafka(ctx, wg)
	if err != nil {
		t.Error(err)
		return
	}

	time.Sleep(2 * time.Second)

	mux := sync.Mutex{}
	consumed := []string{}
	onErrors := []error{}
	failNextDelivery := true

	kafkaConf := Config{
		KafkaUrl:            kafkaUrl,
		ConsumerGroup:       "test",
		Debug:               true,
		InitTopic:           true,
		MessageRetryTimeout: time.Second,
		RestartBackoffMin:   100 * time.Millisecond,
		RestartBackoffMax:   500 * time.Millisecond,
		OnError: func(err error) {
			mux.Lock()
			defer mux.Unlock()
			onErrors = append(onErrors, err)
		},
	}

	err = NewConsumer(ctx, kafkaConf, "test", func(delivery []byte) error {
		mux.Lock()
		defer mux.Unlock()
		if failNextDelivery {
			failNextDelivery = false
			return errors.New("test error")
		}
		consumed = append(consumed, string(delivery))
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

	for _, msg := range []string{"foo", "bar"} {
		err = producer.Produce("key", []byte(msg))
		if err != nil {
			t.Error(err)
			return
		}
	}

	//foo is repeated after the restart because its offset was never committed
	err = waitFor(30*time.Second, func() bool {
		mux.Lock()
		defer mux.Unlock()
		return len(consumed) >= 2
	})
	mux.Lock()
	defer mux.Unlock()
	if err != nil {
		t.Error(err, consumed)
		return
	}
	if !reflect.DeepEqual(consumed, []string{"foo", "bar"}) {
		t.Error(consumed)
		return
	}
	//a disruption this short is bridged by the restart and must not reach OnError
	if len(onErrors) != 0 {
		t.Error(onErrors)
		return
	}
}

// without a consumer-group nothing is committed, so the restart has to keep the
// position in this process: a reader starting over at FirstOffset would deliver
// every earlier message again
func TestKafkaPartitionConsumerKeepsOffsetOverRestart(t *testing.T) {
	wg := &sync.WaitGroup{}
	defer wg.Wait()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	kafkaUrl, err := docker.Kafka(ctx, wg)
	if err != nil {
		t.Error(err)
		return
	}

	time.Sleep(2 * time.Second)

	mux := sync.Mutex{}
	consumed := []string{}
	failedOnce := false

	kafkaConf := Config{
		KafkaUrl:            kafkaUrl,
		Debug:               true,
		InitTopic:           true,
		MessageRetryTimeout: time.Second,
		RestartBackoffMin:   100 * time.Millisecond,
		RestartBackoffMax:   500 * time.Millisecond,
		OnError: func(err error) {
			t.Log("OnError:", err)
		},
	}

	err = NewConsumer(ctx, kafkaConf, "test", func(delivery []byte) error {
		mux.Lock()
		defer mux.Unlock()
		if string(delivery) == "b" && !failedOnce {
			failedOnce = true
			return errors.New("test error")
		}
		consumed = append(consumed, string(delivery))
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

	for _, msg := range []string{"a", "b", "c"} {
		err = producer.Produce(msg, []byte(msg))
		if err != nil {
			t.Error(err)
			return
		}
	}

	err = waitFor(30*time.Second, func() bool {
		mux.Lock()
		defer mux.Unlock()
		return len(consumed) >= 3
	})
	//give a wrongly reset reader the chance to deliver a again
	time.Sleep(2 * time.Second)
	mux.Lock()
	defer mux.Unlock()
	if err != nil {
		t.Error(err, consumed)
		return
	}
	if !reflect.DeepEqual(consumed, []string{"a", "b", "c"}) {
		t.Error(consumed)
		return
	}
}

// a message that never becomes processable keeps the consumer alive and is
// retried with backoff, and the lasting failure reaches OnError
func TestKafkaConsumerReportsLastingError(t *testing.T) {
	wg := &sync.WaitGroup{}
	defer wg.Wait()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	kafkaUrl, err := docker.Kafka(ctx, wg)
	if err != nil {
		t.Error(err)
		return
	}

	time.Sleep(2 * time.Second)

	mux := sync.Mutex{}
	deliveries := 0
	onErrors := []error{}

	kafkaConf := Config{
		KafkaUrl:            kafkaUrl,
		ConsumerGroup:       "test",
		Debug:               true,
		InitTopic:           true,
		MessageRetryTimeout: 100 * time.Millisecond,
		RestartBackoffMin:   50 * time.Millisecond,
		RestartBackoffMax:   200 * time.Millisecond,
		OnError: func(err error) {
			mux.Lock()
			defer mux.Unlock()
			onErrors = append(onErrors, err)
		},
	}

	err = NewConsumer(ctx, kafkaConf, "test", func(delivery []byte) error {
		mux.Lock()
		defer mux.Unlock()
		deliveries++
		return errors.New("test error")
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
	err = producer.Produce("key", []byte("foo"))
	if err != nil {
		t.Error(err)
		return
	}

	err = waitFor(30*time.Second, func() bool {
		mux.Lock()
		defer mux.Unlock()
		return deliveries >= 3 && len(onErrors) >= 1
	})
	mux.Lock()
	defer mux.Unlock()
	if err != nil {
		t.Error(err, "deliveries:", deliveries, "onErrors:", onErrors)
		return
	}
}

func waitFor(timeout time.Duration, condition func() bool) error {
	start := time.Now()
	for time.Since(start) < timeout {
		if condition() {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return errors.New("timeout while waiting for condition")
}
