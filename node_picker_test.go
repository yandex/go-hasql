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
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRandomNodePicker(t *testing.T) {
	t.Run("pick_node", func(t *testing.T) {
		nodes := []CheckedNode[*sql.DB]{
			{
				Node: NewNode("shimba", (*sql.DB)(nil)),
			},
			{
				Node: NewNode("boomba", (*sql.DB)(nil)),
			},
			{
				Node: NewNode("looken", (*sql.DB)(nil)),
			},
		}

		np := new(RandomNodePicker[*sql.DB])

		pickedNodes := make(map[string]struct{})
		for range 100 {
			pickedNodes[np.PickNode(nodes).Node.String()] = struct{}{}
		}
		expectedNodes := map[string]struct{}{"boomba": {}, "looken": {}, "shimba": {}}

		assert.Equal(t, expectedNodes, pickedNodes)
	})

	t.Run("compare_nodes", func(t *testing.T) {
		a := CheckedNode[*sql.DB]{
			Node: NewNode("shimba", (*sql.DB)(nil)),
			Info: NodeInfo{
				ClusterRole:    NodeRolePrimary,
				NetworkLatency: 10 * time.Millisecond,
				ReplicaLag:     1,
			},
		}

		b := CheckedNode[*sql.DB]{
			Node: NewNode("boomba", (*sql.DB)(nil)),
			Info: NodeInfo{
				ClusterRole:    NodeRoleStandby,
				NetworkLatency: 20 * time.Millisecond,
				ReplicaLag:     2,
			},
		}

		np := new(RandomNodePicker[*sql.DB])

		for _, nodeA := range []CheckedNode[*sql.DB]{a, b} {
			for _, nodeB := range []CheckedNode[*sql.DB]{a, b} {
				assert.Equal(t, 0, np.CompareNodes(nodeA, nodeB))
			}
		}
	})
}

func TestRoundRobinNodePicker(t *testing.T) {
	t.Run("pick_node", func(t *testing.T) {
		nodes := []CheckedNode[*sql.DB]{
			{
				Node: NewNode("shimba", (*sql.DB)(nil)),
			},
			{
				Node: NewNode("boomba", (*sql.DB)(nil)),
			},
			{
				Node: NewNode("looken", (*sql.DB)(nil)),
			},
			{
				Node: NewNode("tooken", (*sql.DB)(nil)),
			},
			{
				Node: NewNode("chicken", (*sql.DB)(nil)),
			},
			{
				Node: NewNode("cooken", (*sql.DB)(nil)),
			},
		}

		np := new(RoundRobinNodePicker[*sql.DB])

		var pickedNodes []string
		for range len(nodes) * 3 {
			pickedNodes = append(pickedNodes, np.PickNode(nodes).Node.String())
		}

		expectedNodes := []string{
			"shimba", "boomba", "looken", "tooken", "chicken", "cooken",
			"shimba", "boomba", "looken", "tooken", "chicken", "cooken",
			"shimba", "boomba", "looken", "tooken", "chicken", "cooken",
		}
		assert.Equal(t, expectedNodes, pickedNodes)
	})

	t.Run("compare_nodes", func(t *testing.T) {
		a := CheckedNode[*sql.DB]{
			Node: NewNode("shimba", (*sql.DB)(nil)),
			Info: NodeInfo{
				ClusterRole:    NodeRolePrimary,
				NetworkLatency: 10 * time.Millisecond,
				ReplicaLag:     1,
			},
		}

		b := CheckedNode[*sql.DB]{
			Node: NewNode("boomba", (*sql.DB)(nil)),
			Info: NodeInfo{
				ClusterRole:    NodeRoleStandby,
				NetworkLatency: 20 * time.Millisecond,
				ReplicaLag:     2,
			},
		}

		np := new(RoundRobinNodePicker[*sql.DB])

		assert.Equal(t, 1, np.CompareNodes(a, b))
		assert.Equal(t, -1, np.CompareNodes(b, a))
		assert.Equal(t, 0, np.CompareNodes(a, a))
		assert.Equal(t, 0, np.CompareNodes(b, b))
	})
}

func TestLatencyNodePicker(t *testing.T) {
	t.Run("pick_node", func(t *testing.T) {
		nodes := []CheckedNode[*sql.DB]{
			{
				Node: NewNode("shimba", (*sql.DB)(nil)),
			},
			{
				Node: NewNode("boomba", (*sql.DB)(nil)),
			},
			{
				Node: NewNode("looken", (*sql.DB)(nil)),
			},
			{
				Node: NewNode("tooken", (*sql.DB)(nil)),
			},
			{
				Node: NewNode("chicken", (*sql.DB)(nil)),
			},
			{
				Node: NewNode("cooken", (*sql.DB)(nil)),
			},
		}

		np := new(LatencyNodePicker[*sql.DB])

		pickedNodes := make(map[string]struct{})
		for range 100 {
			pickedNodes[np.PickNode(nodes).Node.String()] = struct{}{}
		}

		expectedNodes := map[string]struct{}{
			"shimba": {},
		}
		assert.Equal(t, expectedNodes, pickedNodes)
	})

	t.Run("compare_nodes", func(t *testing.T) {
		a := CheckedNode[*sql.DB]{
			Node: NewNode("shimba", (*sql.DB)(nil)),
			Info: NodeInfo{
				ClusterRole:    NodeRolePrimary,
				NetworkLatency: 10 * time.Millisecond,
				ReplicaLag:     1,
			},
		}

		b := CheckedNode[*sql.DB]{
			Node: NewNode("boomba", (*sql.DB)(nil)),
			Info: NodeInfo{
				ClusterRole:    NodeRoleStandby,
				NetworkLatency: 20 * time.Millisecond,
				ReplicaLag:     2,
			},
		}

		np := new(LatencyNodePicker[*sql.DB])

		assert.Equal(t, -1, np.CompareNodes(a, b))
		assert.Equal(t, 1, np.CompareNodes(b, a))
		assert.Equal(t, 0, np.CompareNodes(a, a))
		assert.Equal(t, 0, np.CompareNodes(b, b))
	})
}

func TestReplicationNodePicker(t *testing.T) {
	t.Run("pick_node", func(t *testing.T) {
		nodes := []CheckedNode[*sql.DB]{
			{
				Node: NewNode("shimba", (*sql.DB)(nil)),
			},
			{
				Node: NewNode("boomba", (*sql.DB)(nil)),
			},
			{
				Node: NewNode("looken", (*sql.DB)(nil)),
			},
			{
				Node: NewNode("tooken", (*sql.DB)(nil)),
			},
			{
				Node: NewNode("chicken", (*sql.DB)(nil)),
			},
			{
				Node: NewNode("cooken", (*sql.DB)(nil)),
			},
		}

		np := new(ReplicationNodePicker[*sql.DB])

		pickedNodes := make(map[string]struct{})
		for range 100 {
			pickedNodes[np.PickNode(nodes).Node.String()] = struct{}{}
		}

		expectedNodes := map[string]struct{}{
			"shimba": {},
		}
		assert.Equal(t, expectedNodes, pickedNodes)
	})

	t.Run("compare_nodes", func(t *testing.T) {
		a := CheckedNode[*sql.DB]{
			Node: NewNode("shimba", (*sql.DB)(nil)),
			Info: NodeInfo{
				ClusterRole:    NodeRolePrimary,
				NetworkLatency: 10 * time.Millisecond,
				ReplicaLag:     1,
			},
		}

		b := CheckedNode[*sql.DB]{
			Node: NewNode("boomba", (*sql.DB)(nil)),
			Info: NodeInfo{
				ClusterRole:    NodeRoleStandby,
				NetworkLatency: 20 * time.Millisecond,
				ReplicaLag:     2,
			},
		}

		np := new(ReplicationNodePicker[*sql.DB])

		assert.Equal(t, -1, np.CompareNodes(a, b))
		assert.Equal(t, 1, np.CompareNodes(b, a))
		assert.Equal(t, 0, np.CompareNodes(a, a))
		assert.Equal(t, 0, np.CompareNodes(b, b))
	})
}
