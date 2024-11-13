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
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
)

var _ NodeDiscoverer[*sql.DB] = (*mockNodesDiscoverer[*sql.DB])(nil)

// mockNodesDiscoverer returns stored results to tests
type mockNodesDiscoverer[T Querier] struct {
	nodes []*Node[T]
	err   error
}

func (e mockNodesDiscoverer[T]) DiscoverNodes(_ context.Context) ([]*Node[T], error) {
	return slices.Clone(e.nodes), e.err
}

var _ Querier = (*mockQuerier)(nil)

type mockQuerier struct {
	name       string
	queryFn    func(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	queryRowFn func(ctx context.Context, query string, args ...any) *sql.Row
}

func (m *mockQuerier) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	if m.queryFn != nil {
		return m.queryFn(ctx, query, args...)
	}
	return nil, nil
}

func (m *mockQuerier) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	if m.queryRowFn != nil {
		return m.queryRowFn(ctx, query, args...)
	}
	return nil
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

	t.Run("all_nodes_alive", func(t *testing.T) {
		node1 := &Node[*mockQuerier]{
			name: "shimba",
			db:   &mockQuerier{name: "primary"},
		}
		node2 := &Node[*mockQuerier]{
			name: "boomba",
			db:   &mockQuerier{name: "standby1"},
		}
		node3 := &Node[*mockQuerier]{
			name: "looken",
			db:   &mockQuerier{name: "standby2"},
		}

		discoverer := mockNodesDiscoverer[*mockQuerier]{
			nodes: []*Node[*mockQuerier]{node1, node2, node3},
		}

		// mock node checker func
		checkFn := func(_ context.Context, q Querier) (NodeInfoProvider, error) {
			mq, ok := q.(*mockQuerier)
			if !ok {
				return NodeInfo{}, nil
			}

			switch mq.name {
			case node1.db.name:
				return NodeInfo{ClusterRole: NodeRolePrimary, NetworkLatency: 100}, nil
			case node2.db.name:
				return NodeInfo{ClusterRole: NodeRoleStandby, NetworkLatency: 50}, nil
			case node3.db.name:
				return NodeInfo{ClusterRole: NodeRoleStandby, NetworkLatency: 70}, nil
			default:
				return NodeInfo{}, nil
			}
		}

		var picker LatencyNodePicker[*mockQuerier]
		var tracer BaseTracer[*mockQuerier]

		checked := checkNodes(context.Background(), discoverer, checkFn, picker.CompareNodes, tracer)

		expected := CheckedNodes[*mockQuerier]{
			discovered: []*Node[*mockQuerier]{node1, node2, node3},
			alive: []CheckedNode[*mockQuerier]{
				{Node: node2, Info: NodeInfo{ClusterRole: NodeRoleStandby, NetworkLatency: 50}},
				{Node: node3, Info: NodeInfo{ClusterRole: NodeRoleStandby, NetworkLatency: 70}},
				{Node: node1, Info: NodeInfo{ClusterRole: NodeRolePrimary, NetworkLatency: 100}},
			},
			primaries: []CheckedNode[*mockQuerier]{
				{Node: node1, Info: NodeInfo{ClusterRole: NodeRolePrimary, NetworkLatency: 100}},
			},
			standbys: []CheckedNode[*mockQuerier]{
				{Node: node2, Info: NodeInfo{ClusterRole: NodeRoleStandby, NetworkLatency: 50}},
				{Node: node3, Info: NodeInfo{ClusterRole: NodeRoleStandby, NetworkLatency: 70}},
			},
		}

		assert.Equal(t, expected, checked)
	})

	t.Run("all_nodes_dead", func(t *testing.T) {
		node1 := &Node[*mockQuerier]{
			name: "shimba",
			db:   &mockQuerier{name: "primary"},
		}
		node2 := &Node[*mockQuerier]{
			name: "boomba",
			db:   &mockQuerier{name: "standby1"},
		}
		node3 := &Node[*mockQuerier]{
			name: "looken",
			db:   &mockQuerier{name: "standby2"},
		}

		discoverer := mockNodesDiscoverer[*mockQuerier]{
			nodes: []*Node[*mockQuerier]{node1, node2, node3},
		}

		// mock node checker func
		checkFn := func(_ context.Context, q Querier) (NodeInfoProvider, error) {
			return nil, io.EOF
		}

		var picker LatencyNodePicker[*mockQuerier]
		var tracer BaseTracer[*mockQuerier]

		checked := checkNodes(context.Background(), discoverer, checkFn, picker.CompareNodes, tracer)

		expectedDiscovered := []*Node[*mockQuerier]{node1, node2, node3}
		assert.Equal(t, expectedDiscovered, checked.discovered)

		assert.Empty(t, checked.alive)
		assert.Empty(t, checked.primaries)
		assert.Empty(t, checked.standbys)

		var cerrs NodeCheckErrors[*mockQuerier]
		assert.ErrorAs(t, checked.err, &cerrs)
		assert.Len(t, cerrs, 3)
		for _, cerr := range cerrs {
			assert.ErrorIs(t, cerr, io.EOF)
		}
	})

	t.Run("one_standby_is_dead", func(t *testing.T) {
		node1 := &Node[*mockQuerier]{
			name: "shimba",
			db:   &mockQuerier{name: "primary"},
		}
		node2 := &Node[*mockQuerier]{
			name: "boomba",
			db:   &mockQuerier{name: "standby1"},
		}
		node3 := &Node[*mockQuerier]{
			name: "looken",
			db:   &mockQuerier{name: "standby2"},
		}

		discoverer := mockNodesDiscoverer[*mockQuerier]{
			nodes: []*Node[*mockQuerier]{node1, node2, node3},
		}

		// mock node checker func
		checkFn := func(_ context.Context, q Querier) (NodeInfoProvider, error) {
			mq, ok := q.(*mockQuerier)
			if !ok {
				return NodeInfo{}, nil
			}

			switch mq.name {
			case node1.db.name:
				return NodeInfo{ClusterRole: NodeRolePrimary, NetworkLatency: 100}, nil
			case node2.db.name:
				return NodeInfo{ClusterRole: NodeRoleStandby, NetworkLatency: 50}, nil
			case node3.db.name:
				return nil, io.EOF
			default:
				return NodeInfo{}, nil
			}
		}

		var picker LatencyNodePicker[*mockQuerier]
		var tracer BaseTracer[*mockQuerier]

		checked := checkNodes(context.Background(), discoverer, checkFn, picker.CompareNodes, tracer)

		expected := CheckedNodes[*mockQuerier]{
			discovered: []*Node[*mockQuerier]{node1, node2, node3},
			alive: []CheckedNode[*mockQuerier]{
				{Node: node2, Info: NodeInfo{ClusterRole: NodeRoleStandby, NetworkLatency: 50}},
				{Node: node1, Info: NodeInfo{ClusterRole: NodeRolePrimary, NetworkLatency: 100}},
			},
			primaries: []CheckedNode[*mockQuerier]{
				{Node: node1, Info: NodeInfo{ClusterRole: NodeRolePrimary, NetworkLatency: 100}},
			},
			standbys: []CheckedNode[*mockQuerier]{
				{Node: node2, Info: NodeInfo{ClusterRole: NodeRoleStandby, NetworkLatency: 50}},
			},
			err: NodeCheckErrors[*mockQuerier]{
				{node: node3, err: io.EOF},
			},
		}

		assert.Equal(t, expected, checked)
	})

	t.Run("primary_is_dead", func(t *testing.T) {
		node1 := &Node[*mockQuerier]{
			name: "shimba",
			db:   &mockQuerier{name: "primary"},
		}
		node2 := &Node[*mockQuerier]{
			name: "boomba",
			db:   &mockQuerier{name: "standby1"},
		}
		node3 := &Node[*mockQuerier]{
			name: "looken",
			db:   &mockQuerier{name: "standby2"},
		}

		discoverer := mockNodesDiscoverer[*mockQuerier]{
			nodes: []*Node[*mockQuerier]{node1, node2, node3},
		}

		// mock node checker func
		checkFn := func(_ context.Context, q Querier) (NodeInfoProvider, error) {
			mq, ok := q.(*mockQuerier)
			if !ok {
				return NodeInfo{}, nil
			}

			switch mq.name {
			case node1.db.name:
				return nil, io.EOF
			case node2.db.name:
				return NodeInfo{ClusterRole: NodeRoleStandby, NetworkLatency: 70}, nil
			case node3.db.name:
				return NodeInfo{ClusterRole: NodeRoleStandby, NetworkLatency: 50}, nil
			default:
				return NodeInfo{}, nil
			}
		}

		var picker LatencyNodePicker[*mockQuerier]
		var tracer BaseTracer[*mockQuerier]

		checked := checkNodes(context.Background(), discoverer, checkFn, picker.CompareNodes, tracer)

		expected := CheckedNodes[*mockQuerier]{
			discovered: []*Node[*mockQuerier]{node1, node2, node3},
			alive: []CheckedNode[*mockQuerier]{
				{Node: node3, Info: NodeInfo{ClusterRole: NodeRoleStandby, NetworkLatency: 50}},
				{Node: node2, Info: NodeInfo{ClusterRole: NodeRoleStandby, NetworkLatency: 70}},
			},
			primaries: []CheckedNode[*mockQuerier]{},
			standbys: []CheckedNode[*mockQuerier]{
				{Node: node3, Info: NodeInfo{ClusterRole: NodeRoleStandby, NetworkLatency: 50}},
				{Node: node2, Info: NodeInfo{ClusterRole: NodeRoleStandby, NetworkLatency: 70}},
			},
			err: NodeCheckErrors[*mockQuerier]{
				{node: node1, err: io.EOF},
			},
		}

		assert.Equal(t, expected, checked)
	})
}
