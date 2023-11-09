/*
 * Copyright 2022 InfAI (CC SES)
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

package fallback

import (
	"reflect"
	"testing"
)

func TestFallback(t *testing.T) {
	filename := t.TempDir() + "/fallback.json"
	t.Log("use filename=", filename)

	var fallback *Fallback
	var fallback2 *Fallback

	var err error

	t.Run("first init", func(t *testing.T) {
		fallback, err = New(filename)
		if err != nil {
			t.Error(err)
		}
	})

	t.Run("Set value foo 1", testSetFallbackValue(fallback, "foo", float64(1)))
	t.Run("Set value bar batz", testSetFallbackValue(fallback, "bar", "batz"))

	t.Run("check value foo 1", testCheckFallbackValue(fallback, "foo", float64(1)))
	t.Run("check value bar batz", testCheckFallbackValue(fallback, "bar", "batz"))

	t.Run("Set value foo 2", testSetFallbackValue(fallback, "foo", float64(2)))
	t.Run("check value foo 2", testCheckFallbackValue(fallback, "foo", float64(2)))

	t.Run("second init", func(t *testing.T) {
		fallback2, err = New(filename)
		if err != nil {
			t.Error(err)
		}
	})

	t.Run("second check value bar batz", testCheckFallbackValue(fallback2, "bar", "batz"))
	t.Run("second check value foo 2", testCheckFallbackValue(fallback2, "foo", float64(2)))
}

func testCheckFallbackValue(fallback *Fallback, key string, expected interface{}) func(t *testing.T) {
	return func(t *testing.T) {
		actual, err := fallback.Get(key)
		if err != nil {
			t.Error(err)
			return
		}
		if !reflect.DeepEqual(actual, expected) {
			t.Error(actual, expected)
			return
		}
	}
}

func testSetFallbackValue(fallback *Fallback, key string, value interface{}) func(t *testing.T) {
	return func(t *testing.T) {
		err := fallback.Set(key, value)
		if err != nil {
			t.Error(err)
			return
		}
	}
}
