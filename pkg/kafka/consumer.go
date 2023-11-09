/*
 * Copyright (c) 2022 InfAI (CC SES)
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
	"github.com/segmentio/kafka-go"
	"io"
	"log"
	"os"
	"time"
)

func NewConsumer(ctx context.Context, config Config, topic string, listener func(delivery []byte) error) error {
	return NewMultiConsumer(ctx, config, []string{topic}, func(delivery Message) error {
		return listener(delivery.Value)
	})
}

type Message struct {
	Topic         string
	Partition     int
	Offset        int64
	HighWaterMark int64
	Key           []byte
	Value         []byte
	Time          time.Time
}

func NewMultiConsumer(ctx context.Context, config Config, topics []string, listener func(delivery Message) error) (err error) {
	if len(topics) == 0 {
		return nil
	}
	if config.ConsumerGroup == "" {
		return errors.New("missing consumer-group")
	}
	if config.PartitionWatchInterval == 0 {
		config.PartitionWatchInterval = time.Minute
	}

	for _, topic := range topics {
		err = InitTopic(config.KafkaUrl, topic)
		if err != nil {
			log.Println("ERROR: unable to create topic", err)
			return err
		}
	}

	topic := ""
	if len(topics) == 1 {
		topic = topics[0]
		topics = nil
	}

	startTime := time.Now()

	r := kafka.NewReader(kafka.ReaderConfig{
		StartOffset:            config.StartOffset,
		CommitInterval:         0, //synchronous commits
		Brokers:                []string{config.KafkaUrl},
		GroupID:                config.ConsumerGroup,
		GroupTopics:            topics,
		Topic:                  topic,
		MaxWait:                1 * time.Second,
		Logger:                 log.New(io.Discard, "", 0),
		ErrorLogger:            log.New(os.Stdout, "[KAFKA-ERR] ", log.LstdFlags),
		WatchPartitionChanges:  true,
		PartitionWatchInterval: config.PartitionWatchInterval,
	})
	if config.Wg != nil {
		config.Wg.Add(1)
	}
	go func() {
		if config.Wg != nil {
			defer config.Wg.Done()
		}
		defer r.Close()
		defer log.Println("close consumer for topic ", topic)
		for {
			select {
			case <-ctx.Done():
				return
			default:
				m, err := r.FetchMessage(ctx)
				if err == io.EOF || err == context.Canceled {
					return
				}
				if err != nil {
					log.Fatal("ERROR: while consuming topic ", topic, err)
					return
				}
				if !(config.StartOffset == LastOffset && m.Time.Before(startTime)) { //if LastOffset: skip messages, that are older than the start time
					err = retry(func() error {
						return listener(Message{
							Topic:         m.Topic,
							Partition:     m.Partition,
							Offset:        m.Offset,
							HighWaterMark: m.HighWaterMark,
							Key:           m.Key,
							Value:         m.Value,
							Time:          m.Time,
						})
					}, func(n int64) time.Duration {
						return time.Duration(n) * time.Second
					}, 10*time.Minute)
				}

				if err != nil {
					log.Fatal("ERROR: unable to handle message (no commit)", err)
				} else {
					err = r.CommitMessages(ctx, m)
					if err != nil {
						log.Fatal("ERROR: while committing consumption ", topic, err)
						return
					}
				}
			}
		}
	}()
	return nil
}

func retry(f func() error, waitProvider func(n int64) time.Duration, timeout time.Duration) (err error) {
	err = errors.New("initial")
	start := time.Now()
	for i := int64(1); err != nil && time.Since(start) < timeout; i++ {
		err = f()
		if err != nil {
			log.Println("ERROR: kafka listener error:", err)
			wait := waitProvider(i)
			if time.Since(start)+wait < timeout {
				log.Println("ERROR: retry after:", wait.String())
				time.Sleep(wait)
			} else {
				return err
			}
		}
	}
	return err
}
