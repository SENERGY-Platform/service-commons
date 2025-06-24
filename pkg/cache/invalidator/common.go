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
	"encoding/json"
	"errors"
	"fmt"
	"github.com/SENERGY-Platform/service-commons/pkg/kafka"
	"github.com/SENERGY-Platform/service-commons/pkg/signal"
	"log"
)

type KnownTopics struct {
	DeviceTopic         string
	DeviceTypeTopic     string
	ConceptTopic        string
	CharacteristicTopic string
	FunctionTopic       string
	AspectTopic         string
	HubTopic            string
	DeviceClassTopic    string
	DeviceGroupTopic    string
	ProtocolTopic       string
	LocationTopic       string
}

type Command struct {
	Command string `json:"command"`
	Id      string `json:"id"`
}

type SignalInfo struct {
	Signal      signal.Signal
	SignalValue string
}

type SignalMapper = func(message kafka.Message) []SignalInfo

func GetKnownSignalMapper(topics KnownTopics) SignalMapper {
	return func(msg kafka.Message) []SignalInfo {
		cmd := Command{}
		err := json.Unmarshal(msg.Value, &cmd)
		if err != nil {
			log.Printf("ERROR: unable to interpret message for cache invalidation on topic %v: %v \nmessage = %v", msg.Topic, err, string(msg.Value))
			return nil
		}
		var sig signal.Signal
		switch msg.Topic {
		case topics.AspectTopic:
			sig = signal.Known.AspectCacheInvalidation
		case topics.ConceptTopic:
			sig = signal.Known.ConceptCacheInvalidation
		case topics.CharacteristicTopic:
			sig = signal.Known.CharacteristicCacheInvalidation
		case topics.DeviceTopic:
			sig = signal.Known.DeviceCacheInvalidation
		case topics.FunctionTopic:
			sig = signal.Known.FunctionCacheInvalidation
		case topics.HubTopic:
			sig = signal.Known.HubCacheInvalidation
		case topics.DeviceTypeTopic:
			sig = signal.Known.DeviceTypeCacheInvalidation
		case topics.DeviceClassTopic:
			sig = signal.Known.DeviceClassCacheInvalidation
		case topics.DeviceGroupTopic:
			sig = signal.Known.DeviceGroupInvalidation
		case topics.ProtocolTopic:
			sig = signal.Known.ProtocolInvalidation
		case topics.LocationTopic:
			sig = signal.Known.LocationInvalidation
		default:
			sig = signal.Known.GenericCacheInvalidation
			log.Println("WARNING: unknown topic to map in GetKnownSignalMapper()", msg.Topic)
		}
		if cmd.Id != "" {
			return []SignalInfo{{
				Signal:      sig,
				SignalValue: cmd.Id,
			}}
		}
		return nil
	}
}

func GetCommandSignalMapper(sig signal.Signal) SignalMapper {
	return func(msg kafka.Message) []SignalInfo {
		cmd := Command{}
		err := json.Unmarshal(msg.Value, &cmd)
		if err != nil {
			log.Printf("ERROR: unable to interpret message for cache invalidation on topic %v: %v \nmessage = %v", msg.Topic, err, string(msg.Value))
			return nil
		}
		if cmd.Command == "PUT" {
			return []SignalInfo{{
				Signal:      sig,
				SignalValue: cmd.Id,
			}}
		}
		return nil
	}
}

// StartCacheInvalidator starts a kafka consumer, that sends a signal.Signal to 'broker' (signal.DefaultBroker if nil).
// the mapper SignalMapper parameter translates the received kafka messages to SignalInfo which describes the signals sent to the signal.Broker.
// most applications should use StartKnownCacheInvalidators or StartCacheInvalidatorAll
func StartCacheInvalidator(ctx context.Context, kafkaConf kafka.Config, topics []string, mapper SignalMapper, broker *signal.Broker) error {
	if len(topics) == 0 {
		return nil
	}
	if broker == nil {
		broker = signal.DefaultBroker
	}
	if mapper == nil {
		panic(errors.New("missing signal mapper"))
	}
	err := kafka.NewMultiConsumer(ctx, kafkaConf, topics, func(msg kafka.Message) error {
		for _, info := range mapper(msg) {
			broker.Pub(info.Signal, info.SignalValue)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("unable to start kafka consumer for cache invalidation on topic %v: %w", topics, err)
	}
	return nil
}
