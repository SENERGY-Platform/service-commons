/*
 * Copyright 2024 InfAI (CC SES)
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

package donewait

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/SENERGY-Platform/service-commons/pkg/kafka"
	"github.com/SENERGY-Platform/service-commons/pkg/signal"
	"log"
	"strings"
	"sync"
)

type DoneMsg struct {
	ResourceKind string `json:"resource_kind"`
	ResourceId   string `json:"resource_id"`
	Command      string `json:"command"` // PUT | DELETE | RIGHTS
	Handler      string `json:"handler"` // e.g. github.com/SENERGY-Platform/permission-search, may be empty in WaitForDone
}

func SerializeDoneMsg(msg DoneMsg) string {
	return fmt.Sprintf("%v|%v|%v|%v", msg.Command, msg.ResourceKind, msg.ResourceId, msg.Handler)
}

func StartDoneWaitListener(ctx context.Context, kafkaConf kafka.Config, topics []string, broker *signal.Broker) (err error) {
	if broker == nil {
		broker = signal.DefaultBroker
	}
	return kafka.NewMultiConsumer(ctx, kafkaConf, topics, func(msg kafka.Message) error {
		doneMsg := DoneMsg{}
		err := json.Unmarshal(msg.Value, &doneMsg)
		if err != nil {
			log.Printf("ERROR: unable to interpret message for done wait on topic %v: %v \nmessage = %v", msg.Topic, err, string(msg.Value))
			return nil
		}
		broker.Pub(signal.Known.UpdateDone, SerializeDoneMsg(doneMsg))
		return nil
	})
}

func WaitForDone(ctx context.Context, msg DoneMsg, broker *signal.Broker) error {
	if broker == nil {
		broker = signal.DefaultBroker
	}
	expected := SerializeDoneMsg(msg)
	return signal.Wait(ctx, signal.Known.UpdateDone, func(value string) bool {
		return strings.HasPrefix(value, expected) //use prefix in case the handler is irrelevant
	}, broker)
}

func AsyncWait(ctx context.Context, msg DoneMsg, broker *signal.Broker) func() error {
	wg := sync.WaitGroup{}
	var err error
	wg.Add(1)
	go func() {
		defer wg.Done()
		err = WaitForDone(ctx, msg, broker)
	}()
	return func() error {
		wg.Wait()
		return err
	}
}

func AsyncWaitMultiple(ctx context.Context, messages []DoneMsg, broker *signal.Broker) func() error {
	wg := sync.WaitGroup{}
	errList := []error{}
	errMux := sync.Mutex{}
	for _, m := range messages {
		wg.Add(1)
		go func(msg DoneMsg) {
			defer wg.Done()
			err := WaitForDone(ctx, msg, broker)
			if err != nil {
				errMux.Lock()
				defer errMux.Unlock()
				errList = append(errList, err)
			}
		}(m)
	}

	return func() error {
		wg.Wait()
		return errors.Join(errList...)
	}
}
