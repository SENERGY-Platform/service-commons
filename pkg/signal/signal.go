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

type Signal uint

var Known = struct {
	CacheInvalidationAll            Signal
	GenericCacheInvalidation        Signal
	DeviceCacheInvalidation         Signal
	DeviceTypeCacheInvalidation     Signal
	ConceptCacheInvalidation        Signal
	CharacteristicCacheInvalidation Signal
	FunctionCacheInvalidation       Signal
	AspectCacheInvalidation         Signal
	DeviceClassCacheInvalidation    Signal
	HubCacheInvalidation            Signal
}{
	CacheInvalidationAll:            0,
	GenericCacheInvalidation:        1,
	DeviceCacheInvalidation:         2,
	DeviceTypeCacheInvalidation:     3,
	ConceptCacheInvalidation:        4,
	CharacteristicCacheInvalidation: 5,
	FunctionCacheInvalidation:       6,
	AspectCacheInvalidation:         7,
	DeviceClassCacheInvalidation:    8,
	HubCacheInvalidation:            9,
}

func (this Signal) Sub(id string, f func(value string)) string {
	return Sub(id, this, f)
}

func (this Signal) Pub(value string) {
	Pub(this, value)
}
