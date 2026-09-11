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
	if config.MessageRetryTimeout == 0 {
		config.MessageRetryTimeout = 10 * time.Minute
	}
	if config.RestartBackoffMin <= 0 {
		config.RestartBackoffMin = time.Second
	}
	if config.RestartBackoffMax < config.RestartBackoffMin {
		config.RestartBackoffMax = max(time.Minute, config.RestartBackoffMin)
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

	if config.Wg != nil {
		config.Wg.Add(1)
	}
	go func() {
		if config.Wg != nil {
			defer config.Wg.Done()
		}
		defer slog.Info("close kafka-go consumer", "topic", topic)
		runWithRestart(ctx, config, []any{"topic", topic, "topics", topics}, func(progress func()) error {
			return consumeGroup(ctx, config, topic, topics, startTime, progress, listener)
		})
	}()
	return nil
}

// consumeGroup reads until ctx is done (nil result) or until an error makes the
// reader unusable. The error is returned instead of ending the consumer, so
// runWithRestart can build a new reader; the reader resumes at the committed
// offset, which means an unhandled message is repeated rather than skipped.
func consumeGroup(ctx context.Context, config Config, topic string, topics []string, startTime time.Time, progress func(), listener func(delivery Message) error) error {
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
	defer r.Close()
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			m, err := r.FetchMessage(ctx)
			slog.Debug("fetch kafka message", "topic", topic, "partition", m.Partition, "offset", m.Offset, "key", string(m.Key), "value", string(m.Value), "error", err)
			if err != nil {
				if ctx.Err() != nil || errors.Is(err, context.Canceled) {
					return nil
				}
				return fmt.Errorf("while consuming topic: %v %w", topic, err)
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
				}, config.MessageRetryTimeout)
				if err != nil {
					return fmt.Errorf("unable to handle message (no commit): %w", err)
				}
			}
			err = r.CommitMessages(ctx, m)
			if err != nil {
				if ctx.Err() != nil || errors.Is(err, context.Canceled) {
					return nil
				}
				return fmt.Errorf("while committing consumption: %v %w", topic, err)
			}
			progress()
		}
	}
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
	//no offset is committed without a consumer-group, so the position has to
	//survive a restart in this process: a new reader would otherwise fall back
	//to startOffset and read the whole partition again
	offset := startOffset
	runWithRestart(ctx, config, []any{"topic", topic, "partition", partition}, func(progress func()) error {
		return consumePartitionFrom(ctx, config, topic, partition, &offset, progress, listener)
	})
}

func consumePartitionFrom(ctx context.Context, config Config, topic string, partition int, offset *int64, progress func(), listener func(delivery Message) error) error {
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
	err := r.SetOffset(*offset)
	if err != nil {
		return fmt.Errorf("unable to set start offset of topic %v partition %v: %w", topic, partition, err)
	}
	slog.Info("start kafka partition consumer", "topic", topic, "partition", partition, "offset", *offset)
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			m, err := r.FetchMessage(ctx)
			slog.Debug("fetch kafka message", "topic", topic, "partition", partition, "offset", m.Offset, "key", string(m.Key), "value", string(m.Value), "error", err)
			if err != nil {
				if ctx.Err() != nil || errors.Is(err, context.Canceled) {
					return nil
				}
				return fmt.Errorf("while consuming topic: %v %w", topic, err)
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
			}, config.MessageRetryTimeout)
			if err != nil {
				//the offset is left on the failed message, so the restart repeats it
				return fmt.Errorf("unable to handle message: %w", err)
			}
			*offset = m.Offset + 1
			progress()
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

// runWithRestart keeps a consumer running until ctx is done. An error ends the
// current run and a new one is started after a growing wait, instead of ending
// the goroutine and leaving a process that stays healthy and ready while it
// consumes nothing. config.OnError is only called once the consumer has been
// failing for longer than RestartBackoffMax: a disruption short enough to be
// bridged stays a warning, a lasting one is as loud as it was before, including
// the log.Fatal of the default OnError.
func runWithRestart(ctx context.Context, config Config, logArgs []any, f func(progress func()) error) {
	backoffMin := config.RestartBackoffMin
	if backoffMin <= 0 {
		backoffMin = time.Second
	}
	backoffMax := config.RestartBackoffMax
	if backoffMax < backoffMin {
		backoffMax = max(time.Minute, backoffMin)
	}

	wait := backoffMin
	disruptionStart := time.Time{} //zero while the consumer makes progress
	for {
		//progress is called by f after every handled message; f runs in this
		//goroutine, so the closure needs no synchronisation
		err := f(func() {
			disruptionStart = time.Time{}
			wait = backoffMin
		})
		if ctx.Err() != nil {
			return
		}
		if err == nil {
			return
		}
		if disruptionStart.IsZero() {
			disruptionStart = time.Now()
		}
		slog.Warn("restart kafka consumer after error", append(append([]any{}, logArgs...),
			"error", err,
			"wait", wait.String(),
			"disrupted-for", time.Since(disruptionStart).String())...)
		if time.Since(disruptionStart) > backoffMax {
			config.OnError(fmt.Errorf("kafka consumer failing for longer than %v: %w", backoffMax, err))
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
		wait = min(2*wait, backoffMax)
	}
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
