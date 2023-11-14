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

package invalidator

import (
	"context"
	"fmt"
	"github.com/SENERGY-Platform/service-commons/pkg/kafka"
	"github.com/SENERGY-Platform/service-commons/pkg/signal"
	"slices"
)

func StartKnownCacheInvalidators(ctx context.Context, kafkaConf kafka.Config, knownTopics KnownTopics, broker *signal.Broker) error {
	topics := slices.DeleteFunc([]string{
		knownTopics.DeviceTopic,
		knownTopics.DeviceTypeTopic,
		knownTopics.ConceptTopic,
		knownTopics.CharacteristicTopic,
		knownTopics.FunctionTopic,
		knownTopics.AspectTopic,
		knownTopics.HubTopic,
		knownTopics.DeviceClassTopic,
		knownTopics.DeviceGroupTopic,
	}, func(s string) bool {
		return s == ""
	})
	return StartCacheInvalidator(ctx, kafkaConf, topics, GetKnownSignalMapper(knownTopics), broker)
}

// StartCacheInvalidatorDeviceType is manly an example. most should use StartKnownCacheInvalidators or StartCacheInvalidatorAll
func StartCacheInvalidatorDeviceType(ctx context.Context, kafkaConf kafka.Config, deviceTypeTopic string, broker *signal.Broker) error {
	return StartCacheInvalidator(ctx, kafkaConf, []string{deviceTypeTopic}, GetCommandSignalMapper(signal.Known.DeviceTypeCacheInvalidation), broker)
}

func StartCacheInvalidatorAll(ctx context.Context, kafkaConf kafka.Config, topics []string, broker *signal.Broker) error {
	if broker == nil {
		broker = signal.DefaultBroker
	}
	err := kafka.NewMultiConsumer(ctx, kafkaConf, topics, func(_ kafka.Message) error {
		broker.Pub(signal.Known.CacheInvalidationAll, "")
		return nil
	})
	if err != nil {
		return fmt.Errorf("unable to start kafka consumer for cache invalidation on topics %v: %w", topics, err)
	}
	return nil
}
