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
	"io"
	"log"
	"log/slog"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
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
		config.OnError = func(err error) {
			slog.Error("fatal kafka error", "error", err)
			log.Fatal("ERROR:", err)
		}
	}
	if config.InitTopic {
		for _, topic := range topics {
			err = InitTopic(config.KafkaUrl, topic)
			if err != nil {
				slog.Error("unable to create topic", "error", err)
				return err
			}
		}
	}
	if config.ConsumerGroup == "" {
		return newPartitionConsumer(ctx, config, topics, listener)
	} else {
		return newGroupConsumer(ctx, config, topics, listener)
	}
}

// errorLogger routes the internal logging of kafka-go to slog
func errorLogger() *log.Logger {
	result := slog.NewLogLogger(slog.Default().Handler(), slog.LevelWarn)
	result.SetPrefix("[KAFKA-ERR] ")
	return result
}

func newGroupConsumer(ctx context.Context, config Config, topics []string, listener func(delivery Message) error) (err error) {
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
		ErrorLogger:            errorLogger(),
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
		defer slog.Info("close kafka-go consumer", "topic", topic)
		for {
			select {
			case <-ctx.Done():
				return
			default:
				m, err := r.FetchMessage(ctx)
				slog.Debug("fetch kafka message", "topic", topic, "partition", m.Partition, "offset", m.Offset, "key", string(m.Key), "value", string(m.Value), "error", err)
				if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) {
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

// used when no consumer-group is configured: every partition of every topic is
// read by this process and no offset is committed, so each instance sees every
// message
func newPartitionConsumer(ctx context.Context, config Config, topics []string, listener func(delivery Message) error) (err error) {
	if len(topics) == 0 {
		return nil
	}

	//ReaderConfig.StartOffset is only used with a consumer-group, so the offset
	//of a partition reader has to be set explicitly
	startOffset := FirstOffset
	if config.StartOffset == LastOffset {
		startOffset = LastOffset
	}

	if config.Wg != nil {
		config.Wg.Add(1)
	}
	partitionWg := &sync.WaitGroup{}
	go func() {
		if config.Wg != nil {
			defer config.Wg.Done()
		}
		<-ctx.Done()
		partitionWg.Wait()
		slog.Info("close kafka partition consumer", "topics", topics)
	}()

	mux := sync.Mutex{}
	knownPartitions := map[string]map[int]bool{}
	startUnknownPartitions := func(topic string, partitions []int) {
		mux.Lock()
		defer mux.Unlock()
		if _, ok := knownPartitions[topic]; !ok {
			knownPartitions[topic] = map[int]bool{}
		}
		for _, partition := range partitions {
			if knownPartitions[topic][partition] {
				continue
			}
			knownPartitions[topic][partition] = true
			partitionWg.Add(1)
			go consumePartition(ctx, config, topic, partition, startOffset, partitionWg, listener)
		}
	}

	updatePartitions := func() error {
		for _, topic := range topics {
			partitions, err := getPartitions(config.KafkaUrl, topic)
			if err != nil {
				return err
			}
			startUnknownPartitions(topic, partitions)
		}
		return nil
	}
	err = updatePartitions()
	if err != nil {
		return err
	}

	if config.PartitionWatchInterval > 0 {
		ticker := time.NewTicker(config.PartitionWatchInterval)
		go func() {
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					err := updatePartitions()
					if err != nil {
						slog.Error("unable to update partition info", "error", err)
					}
				}
			}
		}()
	}

	return nil
}

func consumePartition(ctx context.Context, config Config, topic string, partition int, startOffset int64, wg *sync.WaitGroup, listener func(delivery Message) error) {
	defer wg.Done()
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{config.KafkaUrl},
		Topic:       topic,
		Partition:   partition,
		MaxWait:     1 * time.Second,
		Logger:      log.New(io.Discard, "", 0),
		ErrorLogger: errorLogger(),
	})
	defer func() {
		slog.Info("close kafka partition consumer", "topic", topic, "partition", partition, "result", r.Close())
	}()
	err := r.SetOffset(startOffset)
	if err != nil {
		config.OnError(fmt.Errorf("unable to set start offset of topic %v partition %v: %w", topic, partition, err))
		return
	}
	slog.Info("start kafka partition consumer", "topic", topic, "partition", partition)
	for {
		select {
		case <-ctx.Done():
			return
		default:
			m, err := r.FetchMessage(ctx)
			slog.Debug("fetch kafka message", "topic", topic, "partition", partition, "offset", m.Offset, "key", string(m.Key), "value", string(m.Value), "error", err)
			if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) {
				return
			}
			if err != nil {
				config.OnError(fmt.Errorf("while consuming topic: %v %w", topic, err))
				return
			}
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
			if err != nil {
				config.OnError(fmt.Errorf("unable to handle message: %w", err))
				return
			}
		}
	}
}

// getPartitions retries UnknownTopicOrPartition, because the metadata request
// kafka-go sends carries the auto-create flag: the request that reports the
// missing topic is also the one that makes the broker create it, so only the
// first attempt fails. sarama hid the same round trip behind Metadata.Retry
// (3 attempts, 250ms apart), which is where these numbers come from.
func getPartitions(bootstrapUrl string, topic string) (result []int, err error) {
	conn, err := kafka.Dial("tcp", bootstrapUrl)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	var partitions []kafka.Partition
	for attempt := 0; attempt <= 3; attempt++ {
		if attempt > 0 {
			time.Sleep(250 * time.Millisecond)
		}
		partitions, err = conn.ReadPartitions(topic)
		if !errors.Is(err, kafka.UnknownTopicOrPartition) {
			break
		}
	}
	if err != nil {
		return nil, err
	}
	for _, partition := range partitions {
		result = append(result, partition.ID)
	}
	return result, nil
}

func retry(f func() error, waitProvider func(n int64) time.Duration, timeout time.Duration) (err error) {
	err = errors.New("initial")
	start := time.Now()
	for i := int64(1); err != nil && time.Since(start) < timeout; i++ {
		err = f()
		if err != nil {
			slog.Error("kafka listener error", "error", err)
			wait := waitProvider(i)
			if time.Since(start)+wait < timeout {
				slog.Error("kafka listener error --> retry", "error", err, "wait", wait.String(), "first-try-time", start.String())
				time.Sleep(wait)
			} else {
				return err
			}
		}
	}
	return err
}
