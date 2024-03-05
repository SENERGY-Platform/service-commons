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

package interfaces

import "time"

type CacheImpl interface {
	Get(key string) (value interface{}, generic bool, err error)
	GetWithExpiration(key string) (value interface{}, generic bool, exp time.Duration, err error)
	Set(key string, value interface{}, exp time.Duration) (err error)
	Remove(key string) error
	Reset() error
	Close() error
}
