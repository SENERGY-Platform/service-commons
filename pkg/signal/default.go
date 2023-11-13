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

import "sync"

var DefaultBroker = &Broker{}

func Pub(signal Signal, value string) {
	DefaultBroker.Pub(signal, value)
}

// Sub returns id, if id == "", one will be created
// wg will be done as soon as all subscription functions returned
// on wg only Wait() may be called. and only within a separate go routine, to prevent a deadlock
func Sub(id string, signal Signal, f func(value string, wg *sync.WaitGroup)) string {
	return DefaultBroker.Sub(id, signal, f)
}

func Unsub(id string) {
	DefaultBroker.Unsub(id)
}
