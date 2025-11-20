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
	"bytes"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestSubWg(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	Known.DeviceTypeCacheInvalidation.Sub("", func(value string, _ *sync.WaitGroup) {
		time.Sleep(200 * time.Millisecond)
		fmt.Fprintln(buf, "bar", value)
	})
	Known.DeviceTypeCacheInvalidation.Sub("", func(value string, wg *sync.WaitGroup) {
		go func() {
			wg.Wait()
			fmt.Fprintln(buf, "batz", value)
		}()
	})
	Known.DeviceTypeCacheInvalidation.Sub("", func(value string, _ *sync.WaitGroup) {
		fmt.Fprintln(buf, "blub", value)
	})
	Known.DeviceTypeCacheInvalidation.Sub("", func(value string, _ *sync.WaitGroup) {
		time.Sleep(100 * time.Millisecond)
		fmt.Fprintln(buf, "foo", value)
	})
	Known.DeviceTypeCacheInvalidation.Pub("42")
	time.Sleep(time.Second)

	expected := `blub 42
foo 42
bar 42
batz 42
`

	actual := buf.String()
	if actual != expected {
		t.Error(actual)
	}
}
