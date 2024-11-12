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

import (
	"context"
	"database/sql"
)

// NodeDiscoverer represents a provider of cluster nodes list.
// NodeDiscoverer must node check nodes liveness or role, just return all nodes registered in cluster
type NodeDiscoverer[T Querier] interface {
	// DiscoverNodes returns list of nodes registered in cluster
	DiscoverNodes(context.Context) ([]*Node[T], error)
}

// StaticNodeDiscoverer implements NodeDiscoverer
var _ NodeDiscoverer[*sql.DB] = (*StaticNodeDiscoverer[*sql.DB])(nil)

// StaticNodeDiscoverer returns always returns list of provided nodes
type StaticNodeDiscoverer[T Querier] struct {
	nodes []*Node[T]
}

// NewStaticNodeDiscoverer returns new staticNodeDiscoverer instance
func NewStaticNodeDiscoverer[T Querier](nodes []*Node[T]) StaticNodeDiscoverer[T] {
	return StaticNodeDiscoverer[T]{nodes: nodes}
}

func (s StaticNodeDiscoverer[T]) DiscoverNodes(_ context.Context) ([]*Node[T], error) {
	return s.nodes, nil
}
