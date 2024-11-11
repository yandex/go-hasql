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
		db, dbmock, err := sqlmock.New()
		require.NoError(t, err)
		defer db.Close()

		// expect database client to be closed
		dbmock.ExpectClose()

		nodes := []*Node[*sql.DB]{
			NewNode("shimba", db),
		}

		cl, err := NewCluster(NewStaticNodeDiscoverer(nodes), PostgreSQLChecker)
		defer func() { require.NoError(t, cl.Close()) }()

		assert.NoError(t, err)
		assert.NotNil(t, cl)
	})
}

func TestClusterClose(t *testing.T) {
	t.Run("no_errors", func(t *testing.T) {
		db1, dbmock1, err := sqlmock.New()
		require.NoError(t, err)
		defer db1.Close()

		db2, dbmock2, err := sqlmock.New()
		require.NoError(t, err)
		defer db1.Close()

		// expect database client to be closed
		dbmock1.ExpectClose()
		dbmock2.ExpectClose()

		nodes := []*Node[*sql.DB]{
			NewNode("shimba", db1),
			NewNode("boomba", db2),
		}

		cl, err := NewCluster(NewStaticNodeDiscoverer(nodes), PostgreSQLChecker)
		require.NoError(t, err)

		cl.checkedNodes.Store(CheckedNodes[*sql.DB]{
			discovered: nodes,
		})

		assert.NoError(t, cl.Close())
	})

	t.Run("multiple_errors", func(t *testing.T) {
		db1, dbmock1, err := sqlmock.New()
		require.NoError(t, err)
		defer db1.Close()

		db2, dbmock2, err := sqlmock.New()
		require.NoError(t, err)
		defer db1.Close()

		// expect database client to be closed
		dbmock1.ExpectClose().WillReturnError(io.EOF)
		dbmock2.ExpectClose().WillReturnError(sql.ErrTxDone)

		nodes := []*Node[*sql.DB]{
			NewNode("shimba", db1),
			NewNode("boomba", db2),
		}

		cl, err := NewCluster(NewStaticNodeDiscoverer(nodes), PostgreSQLChecker)
		require.NoError(t, err)

		cl.checkedNodes.Store(CheckedNodes[*sql.DB]{
			discovered: nodes,
		})

		err = cl.Close()
		assert.ErrorIs(t, err, io.EOF)
		assert.ErrorIs(t, err, sql.ErrTxDone)
	})
}

func TestClusterErr(t *testing.T) {
	t.Run("no_error", func(t *testing.T) {
		cl := new(Cluster[*sql.DB])
		cl.checkedNodes.Store(CheckedNodes[*sql.DB]{})
		assert.NoError(t, cl.Err(), io.EOF)
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
