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
	"fmt"
	"github.com/IBM/sarama"
	"github.com/segmentio/kafka-go"
	"io"
	"log"
	"os"
	"runtime/debug"
	"slices"
	"sync"
	"time"
)

func NewConsumer(ctx context.Context, config Config, topic string, listener func(delivery []byte) error) error {
	return NewMultiConsumer(ctx, config, []string{topic}, func(delivery Message) error {
		return listener(delivery.Value)
	})
}

type Message struct {
	Topic     string
	Partition int
	Offset    int64
	Key       []byte
	Value     []byte
	Time      time.Time
}

func NewMultiConsumer(ctx context.Context, config Config, topics []string, listener func(delivery Message) error) (err error) {
	if len(topics) == 0 {
		return nil
	}
	if config.PartitionWatchInterval == 0 {
		config.PartitionWatchInterval = time.Minute
	}
	if config.OnError == nil {
		config.OnError = func(err error) { log.Fatal("ERROR:", err) }
	}
	if config.InitTopic {
		for _, topic := range topics {
			err = InitTopic(config.KafkaUrl, topic)
			if err != nil {
				log.Println("ERROR: unable to create topic", err)
				return err
			}
		}
	}
	if config.ConsumerGroup == "" {
		return newSaramaConsumer(ctx, config, topics, listener)
	} else {
		return newKafkaGoConsumer(ctx, config, topics, listener)
	}
}

func newKafkaGoConsumer(ctx context.Context, config Config, topics []string, listener func(delivery Message) error) (err error) {
	if len(topics) == 0 {
		return nil
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
					config.OnError(fmt.Errorf("while consuming topic: %v %w", topic, err))
					return
				}
				if !(config.StartOffset == LastOffset && m.Time.Before(startTime)) { //if LastOffset: skip messages, that are older than the start time
					err = retry(func() error {
						return listener(Message{
							Topic:     m.Topic,
							Partition: m.Partition,
							Offset:    m.Offset,
							Key:       m.Key,
							Value:     m.Value,
							Time:      m.Time,
						})
					}, func(n int64) time.Duration {
						return time.Duration(n) * time.Second
					}, 10*time.Minute)
				}

				if err != nil {
					config.OnError(fmt.Errorf("unable to handle message (no commit): %w", err))
					return
				} else {
					err = r.CommitMessages(ctx, m)
					if err != nil {
						config.OnError(fmt.Errorf("while committing consumption: %v %w", topic, err))
						return
					}
				}
			}
		}
	}()
	return nil
}

// used when no consumer-group is configured
func newSaramaConsumer(ctx context.Context, config Config, topics []string, listener func(delivery Message) error) (err error) {
	if len(topics) == 0 {
		return nil
	}

	saramaConfig := sarama.NewConfig()
	saramaConfig.Consumer.Return.Errors = true
	switch config.StartOffset {
	case LastOffset:
		saramaConfig.Consumer.Offsets.Initial = sarama.OffsetNewest
	case FirstOffset:
		saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	default:
		saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	}
	if buildInfo, ok := debug.ReadBuildInfo(); ok {
		saramaConfig.ClientID = buildInfo.Path
	}

	client, err := sarama.NewConsumer([]string{config.KafkaUrl}, saramaConfig)
	if err != nil {
		return err
	}

	if config.Wg != nil {
		config.Wg.Add(1)
	}
	partitionWg := sync.WaitGroup{}
	go func() {
		if config.Wg != nil {
			defer config.Wg.Done()
		}
		<-ctx.Done()
		partitionWg.Wait()
		log.Printf("close consumer for topics=%v err=%v\n", topics, client.Close())
	}()

	mux := sync.Mutex{}
	partitionMap := map[string][]int32{}
	handleNewPartitionInfo := func(topic string, partitions []int32) error {
		mux.Lock()
		defer mux.Unlock()
		slices.Sort(partitions)
		current := partitionMap[topic]
		newPartitions := partitions[len(current):]
		if len(newPartitions) > 0 {
			partitionMap[topic] = partitions
			for _, partition := range newPartitions {
				consumer, err := client.ConsumePartition(topic, partition, saramaConfig.Consumer.Offsets.Initial)
				if err != nil {
					return err
				}
				partitionWg.Add(1)
				go func() {
					<-ctx.Done()
					defer log.Printf("closing consumer for topic=%v partition=%v err=%v\n", topic, partition, consumer.Close())
				}()

				go func(topic string, partition int32, consumer sarama.PartitionConsumer) {
					log.Printf("consume topic=%v partition=%v\n", topic, partition)
					defer partitionWg.Done()
					defer log.Printf("closed consumer for topic=%v partition=%v\n", topic, partition)
					for {
						select {
						case <-ctx.Done():
							return
						case consumerError, ok := <-consumer.Errors():
							if !ok {
								return
							}
							if consumerError != nil {
								config.OnError(consumerError)
								return
							}
						case msg, ok := <-consumer.Messages():
							if !ok {
								return
							}
							if config.Debug && msg != nil {
								log.Printf("DEBUG: receive topic=%v partition=%v key=%v message=%v\n", topic, partition, string(msg.Key), string(msg.Value))
							}
							if msg != nil {
								err = retry(func() error {
									return listener(Message{
										Topic:     msg.Topic,
										Partition: int(msg.Partition),
										Offset:    msg.Offset,
										Key:       msg.Key,
										Value:     msg.Value,
										Time:      msg.Timestamp,
									})
								}, func(n int64) time.Duration {
									return time.Duration(n) * time.Second
								}, 10*time.Minute)
								if err != nil {
									config.OnError(fmt.Errorf("unable to handle message (no commit): %w", err))
									return
								}
							}
						}
					}
				}(topic, partition, consumer)
			}
		}
		return nil
	}

	updatePartitions := func(topics []string) error {
		for _, topic := range topics {
			partitions, err := client.Partitions(topic)
			if err != nil {
				return err
			}
			err = handleNewPartitionInfo(topic, partitions)
			if err != nil {
				return err
			}
		}
		return nil
	}
	err = updatePartitions(topics)
	if err != nil {
		return err
	}

	if config.PartitionWatchInterval > 0 {
		ticker := time.NewTicker(config.PartitionWatchInterval)
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					err = updatePartitions(topics)
					if err != nil {
						log.Println("ERROR: unable to update partition info", err)
					}
				}
			}
		}()
	}

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
