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
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

// mockNodesDiscoverer returns stored results to tests
type mockNodesDiscoverer[T Querier] struct {
	nodes []*Node[T]
	err   error
}

func (e mockNodesDiscoverer[T]) DiscoverNodes(_ context.Context) ([]*Node[T], error) {
	return e.nodes, e.err
}

func TestCheckNodes(t *testing.T) {
	t.Run("discovery_error", func(t *testing.T) {
		discoverer := mockNodesDiscoverer[*sql.DB]{
			err: io.EOF,
		}

		nodes := checkNodes(context.Background(), discoverer, nil, nil, nil)
		assert.Empty(t, nodes.discovered)
		assert.Empty(t, nodes.alive)
		assert.Empty(t, nodes.primaries)
		assert.Empty(t, nodes.standbys)
		assert.ErrorIs(t, nodes.err, io.EOF)
	})
}
