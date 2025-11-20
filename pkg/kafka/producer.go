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
	"log"
	"os"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

type Producer struct {
	writer *kafka.Writer
	ctx    context.Context
	config Config
}

func NewProducer(ctx context.Context, config Config, topic string) (*Producer, error) {
	return NewProducerWithBalancer(ctx, config, topic, &kafka.Hash{})
}

func NewProducerWithKeySeparationBalancer(ctx context.Context, config Config, topic string) (*Producer, error) {
	return NewProducerWithBalancer(ctx, config, topic, &KeySeparationBalancer{SubBalancer: &kafka.Hash{}, Seperator: "/"})
}

func NewProducerWithBalancer(ctx context.Context, config Config, topic string, balancer kafka.Balancer) (*Producer, error) {
	result := &Producer{ctx: ctx}
	if config.InitTopic {
		err := InitTopic(config.KafkaUrl, topic)
		if err != nil {
			log.Println("ERROR: unable to create topic", err)
			return nil, err
		}
	}

	var logger kafka.Logger
	if config.Debug {
		logger = log.New(os.Stdout, "KAFKA", 0)
	}

	result.writer = &kafka.Writer{
		Addr:        kafka.TCP(config.KafkaUrl),
		Topic:       topic,
		Async:       false,
		Logger:      logger,
		ErrorLogger: log.New(os.Stderr, "KAFKA", 0),
		MaxAttempts: 10,
		BatchSize:   1,
		Balancer:    balancer,
	}

	if config.Wg != nil {
		config.Wg.Add(1)
	}
	go func() {
		if config.Wg != nil {
			defer config.Wg.Done()
		}
		<-ctx.Done()
		result.writer.Close()
	}()
	return result, nil
}

func (this *Producer) Produce(key string, message []byte) error {
	now := time.Now()
	if this.config.TimeNow != nil {
		now = this.config.TimeNow()
	}
	return this.writer.WriteMessages(this.ctx, kafka.Message{
		Key:   []byte(key),
		Value: message,
		Time:  now,
	})
}

type KeySeparationBalancer struct {
	SubBalancer kafka.Balancer
	Seperator   string
}

func (this *KeySeparationBalancer) Balance(msg kafka.Message, partitions ...int) (partition int) {
	key := string(msg.Key)
	if this.Seperator != "" {
		keyParts := strings.Split(key, this.Seperator)
		key = keyParts[0]
	}
	msg.Key = []byte(key)
	return this.SubBalancer.Balance(msg, partitions...)
}
