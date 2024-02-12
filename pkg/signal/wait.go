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

package signal

import (
	"context"
	"errors"
	"sync"
)

// Wait waits until a signal matching 'sig' and 'is' is received from 'broker'
// broke may be nil (defaults to signal.DefaultBroker)
// returns context.DeadlineExceeded if ctx exceeds a timeout or deadline
// returns nil if ctx is cancelled or the signal is received
func Wait(ctx context.Context, sig Signal, is func(value string) bool, broker *Broker) error {
	if broker == nil {
		broker = DefaultBroker
	}
	subId := ""
	ctx, done := context.WithCancel(ctx)
	subId = broker.Sub(subId, sig, func(value string, wg *sync.WaitGroup) {
		if is(value) {
			done()
		}
	})
	defer broker.Unsub(subId)
	<-ctx.Done()
	err := ctx.Err()
	if err == nil || errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

func (this Signal) Wait(ctx context.Context, is func(value string) bool, broker *Broker) error {
	return Wait(ctx, this, is, broker)
}
