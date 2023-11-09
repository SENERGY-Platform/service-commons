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

package signal

import (
	"github.com/google/uuid"
	"log"
	"runtime/debug"
	"slices"
	"sync"
)

type Broker struct {
	subscriptions []Subscription
	mux           sync.Mutex
	Debug         bool
}

type Subscription struct {
	Id     string
	Signal Signal
	F      func(value string)
}

func (this *Broker) Pub(signal Signal, value string) {
	this.mux.Lock()
	defer this.mux.Unlock()
	for _, sub := range this.subscriptions {
		if sub.Signal == signal {
			if this.Debug {
				log.Println("DEBUG: send signal", sub.Id, signal, value)
			}
			go func(f func(value string)) {
				defer func() {
					if r := recover(); r != nil {
						log.Println("ERROR:", r)
						debug.PrintStack()
					}
				}()
				f(value)
			}(sub.F)
		}
	}
}

// Sub returns id, if id == "", one will be created
func (this *Broker) Sub(id string, signal Signal, f func(value string)) string {
	defer func() {
		if r := recover(); r != nil {
			log.Println("ERROR:", r)
			debug.PrintStack()
		}
	}()
	if id == "" {
		id = uuid.NewString()
	}
	this.mux.Lock()
	defer this.mux.Unlock()
	this.subscriptions = append(this.subscriptions, Subscription{
		Id:     id,
		Signal: signal,
		F:      f,
	})
	return id
}

func (this *Broker) Unsub(id string) {
	this.mux.Lock()
	defer this.mux.Unlock()
	this.subscriptions = slices.DeleteFunc(this.subscriptions, func(sub Subscription) bool {
		return sub.Id == id
	})
}
