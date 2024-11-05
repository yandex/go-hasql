/*
   Copyright 2020 YANDEX LLC

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package hasql

import "time"

// ClusterOpt is a functional option type for Cluster constructor
type ClusterOpt[T Querier] func(*Cluster[T])

// WithUpdateInterval sets interval between cluster state updates
func WithUpdateInterval[T Querier](d time.Duration) ClusterOpt[T] {
	return func(cl *Cluster[T]) {
		cl.updateInterval = d
	}
}

// WithUpdateTimeout sets timeout for update of each node in cluster
func WithUpdateTimeout[T Querier](d time.Duration) ClusterOpt[T] {
	return func(cl *Cluster[T]) {
		cl.updateTimeout = d
	}
}

// WithNodePicker sets algorithm for node selection (e.g. random, round robin etc)
func WithNodePicker[T Querier](picker NodePicker[T]) ClusterOpt[T] {
	return func(cl *Cluster[T]) {
		cl.picker = picker
	}
}

// WithTracer sets tracer for actions happening in the background
func WithTracer[T Querier](tracer Tracer[T]) ClusterOpt[T] {
	return func(cl *Cluster[T]) {
		cl.tracer = tracer
	}
}
