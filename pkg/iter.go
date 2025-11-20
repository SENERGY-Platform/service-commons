/*
 * Copyright 2025 InfAI (CC SES)
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

package pkg

import "iter"

func IterBatch[T any](batchsize int64, getBatch func(limit int64, offset int64) ([]T, error)) iter.Seq2[T, error] {
	var offset int64 = 0
	return func(yield func(T, error) bool) {
		finished := false
		for !finished {
			batch, err := getBatch(batchsize, offset)
			if err != nil {
				var element T
				yield(element, err)
				return
			}
			for _, instance := range batch {
				if !yield(instance, nil) {
					return
				}
			}
			offset += batchsize
			if len(batch) < int(batchsize) {
				finished = true
			}
		}
	}
}
