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
	"io"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewCluster(t *testing.T) {
	t.Run("no_nodes", func(t *testing.T) {
		cl, err := NewCluster[*sql.DB](nil, PostgreSQLChecker)
		assert.Nil(t, cl)
		assert.EqualError(t, err, "node discoverer required")
	})

	t.Run("success", func(t *testing.T) {
		db, _, err := sqlmock.New()
		require.NoError(t, err)

		node := NewNode("shimba", db)

		cl, err := NewCluster(NewStaticNodeDiscoverer(node), PostgreSQLChecker)
		assert.NoError(t, err)
		assert.NotNil(t, cl)
	})
}

func TestCluster_Close(t *testing.T) {
	t.Run("no_errors", func(t *testing.T) {
		db1, dbmock1, err := sqlmock.New()
		require.NoError(t, err)

		db2, dbmock2, err := sqlmock.New()
		require.NoError(t, err)

		// expect database client to be closed
		dbmock1.ExpectClose()
		dbmock2.ExpectClose()

		node1 := NewNode("shimba", db1)
		node2 := NewNode("boomba", db2)

		cl, err := NewCluster(NewStaticNodeDiscoverer(node1, node2), PostgreSQLChecker)
		require.NoError(t, err)

		cl.checkedNodes.Store(CheckedNodes[*sql.DB]{
			discovered: []*Node[*sql.DB]{node1, node2},
		})

		assert.NoError(t, cl.Close())
	})

	t.Run("multiple_errors", func(t *testing.T) {
		db1, dbmock1, err := sqlmock.New()
		require.NoError(t, err)

		db2, dbmock2, err := sqlmock.New()
		require.NoError(t, err)

		// expect database client to be closed
		dbmock1.ExpectClose().WillReturnError(io.EOF)
		dbmock2.ExpectClose().WillReturnError(sql.ErrTxDone)

		node1 := NewNode("shimba", db1)
		node2 := NewNode("boomba", db2)

		cl, err := NewCluster(NewStaticNodeDiscoverer(node1, node2), PostgreSQLChecker)
		require.NoError(t, err)

		cl.checkedNodes.Store(CheckedNodes[*sql.DB]{
			discovered: []*Node[*sql.DB]{node1, node2},
		})

		err = cl.Close()
		assert.ErrorIs(t, err, io.EOF)
		assert.ErrorIs(t, err, sql.ErrTxDone)
	})
}

func TestCluster_Err(t *testing.T) {
	t.Run("no_error", func(t *testing.T) {
		cl := new(Cluster[*sql.DB])
		cl.checkedNodes.Store(CheckedNodes[*sql.DB]{})
		assert.NoError(t, cl.Err())
	})

	t.Run("has_error", func(t *testing.T) {
		checkedNodes := CheckedNodes[*sql.DB]{
			err: NodeCheckErrors[*sql.DB]{
				{
					node: &Node[*sql.DB]{
						name: "shimba",
						db:   new(sql.DB),
					},
					err: io.EOF,
				},
			},
		}

		cl := new(Cluster[*sql.DB])
		cl.checkedNodes.Store(checkedNodes)

		assert.ErrorIs(t, cl.Err(), io.EOF)
	})
}

func TestCluster_Node(t *testing.T) {
	t.Run("no_nodes", func(t *testing.T) {
		cl := new(Cluster[*sql.DB])
		cl.checkedNodes.Store(CheckedNodes[*sql.DB]{})

		// all criteria must return nil node
		for i := Alive; i < maxNodeCriterion; i++ {
			node := cl.Node(i)
			assert.Nil(t, node)
		}
	})

	t.Run("alive", func(t *testing.T) {
		node := &Node[*sql.DB]{
			name: "shimba",
			db:   new(sql.DB),
		}

		cl := new(Cluster[*sql.DB])
		cl.picker = new(RandomNodePicker[*sql.DB])
		cl.checkedNodes.Store(CheckedNodes[*sql.DB]{
			alive: []CheckedNode[*sql.DB]{
				{
					Node: node,
					Info: NodeInfo{
						ClusterRole: NodeRolePrimary,
					},
				},
			},
		})

		assert.Equal(t, node, cl.Node(Alive))
	})

	t.Run("primary", func(t *testing.T) {
		node := &Node[*sql.DB]{
			name: "shimba",
			db:   new(sql.DB),
		}

		cl := new(Cluster[*sql.DB])
		cl.picker = new(RandomNodePicker[*sql.DB])
		cl.checkedNodes.Store(CheckedNodes[*sql.DB]{
			primaries: []CheckedNode[*sql.DB]{
				{
					Node: node,
					Info: NodeInfo{
						ClusterRole: NodeRolePrimary,
					},
				},
			},
		})

		assert.Equal(t, node, cl.Node(Primary))
		// we will return node on Prefer* creterias also
		assert.Equal(t, node, cl.Node(PreferPrimary))
		assert.Equal(t, node, cl.Node(PreferStandby))
	})

	t.Run("standby", func(t *testing.T) {
		node := &Node[*sql.DB]{
			name: "shimba",
			db:   new(sql.DB),
		}

		cl := new(Cluster[*sql.DB])
		cl.picker = new(RandomNodePicker[*sql.DB])
		cl.checkedNodes.Store(CheckedNodes[*sql.DB]{
			standbys: []CheckedNode[*sql.DB]{
				{
					Node: node,
					Info: NodeInfo{
						ClusterRole: NodeRoleStandby,
					},
				},
			},
		})

		assert.Equal(t, node, cl.Node(Standby))
		// we will return node on Prefer* creterias also
		assert.Equal(t, node, cl.Node(PreferPrimary))
		assert.Equal(t, node, cl.Node(PreferStandby))
	})

	t.Run("prefer_primary", func(t *testing.T) {
		node := &Node[*sql.DB]{
			name: "shimba",
			db:   new(sql.DB),
		}

		cl := new(Cluster[*sql.DB])
		cl.picker = new(RandomNodePicker[*sql.DB])

		// must pick from primaries
		cl.checkedNodes.Store(CheckedNodes[*sql.DB]{
			primaries: []CheckedNode[*sql.DB]{
				{
					Node: node,
					Info: NodeInfo{
						ClusterRole: NodeRolePrimary,
					},
				},
			},
		})
		assert.Equal(t, node, cl.Node(PreferPrimary))

		// must pick from standbys
		cl.checkedNodes.Store(CheckedNodes[*sql.DB]{
			standbys: []CheckedNode[*sql.DB]{
				{
					Node: node,
					Info: NodeInfo{
						ClusterRole: NodeRoleStandby,
					},
				},
			},
		})
		assert.Equal(t, node, cl.Node(PreferPrimary))
	})

	t.Run("prefer_standby", func(t *testing.T) {
		node := &Node[*sql.DB]{
			name: "shimba",
			db:   new(sql.DB),
		}

		cl := new(Cluster[*sql.DB])
		cl.picker = new(RandomNodePicker[*sql.DB])

		// must pick from standbys
		cl.checkedNodes.Store(CheckedNodes[*sql.DB]{
			standbys: []CheckedNode[*sql.DB]{
				{
					Node: node,
					Info: NodeInfo{
						ClusterRole: NodeRoleStandby,
					},
				},
			},
		})
		assert.Equal(t, node, cl.Node(PreferStandby))

		// must pick from primaries
		cl.checkedNodes.Store(CheckedNodes[*sql.DB]{
			primaries: []CheckedNode[*sql.DB]{
				{
					Node: node,
					Info: NodeInfo{
						ClusterRole: NodeRolePrimary,
					},
				},
			},
		})
		assert.Equal(t, node, cl.Node(PreferStandby))
	})
}
