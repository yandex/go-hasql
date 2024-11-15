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

package hasql_test

import (
	"context"
	"database/sql"
	"io"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"golang.yandex/hasql/v2"
)

// TestEnd2End_AliveCluster setups 3 node cluster, waits for at least one
// alive node, then picks primary and secondary node. Nodes are always alive.
func TestEnd2End_AliveCluster(t *testing.T) {
	// create three database pools
	db1, mock1, err := sqlmock.New()
	require.NoError(t, err)
	db2, mock2, err := sqlmock.New()
	require.NoError(t, err)
	db3, mock3, err := sqlmock.New()
	require.NoError(t, err)

	// set db1 to be primary node
	mock1.ExpectQuery(`SELECT.*pg_is_in_recovery`).
		WillReturnRows(sqlmock.
			NewRows([]string{"role", "lag"}).
			AddRow(1, 0),
		)

	// set db2 and db3 to be standby nodes
	mock2.ExpectQuery(`SELECT.*pg_is_in_recovery`).
		WillReturnRows(sqlmock.
			NewRows([]string{"role", "lag"}).
			AddRow(2, 0),
		)
	mock3.ExpectQuery(`SELECT.*pg_is_in_recovery`).
		WillReturnRows(sqlmock.
			NewRows([]string{"role", "lag"}).
			AddRow(2, 10),
		)

	// all pools must be closed in the end
	mock1.ExpectClose()
	mock2.ExpectClose()
	mock3.ExpectClose()

	// register pools as nodes
	node1 := hasql.NewNode("ololo", db1)
	node2 := hasql.NewNode("trololo", db2)
	node3 := hasql.NewNode("shimba", db3)
	discoverer := hasql.NewStaticNodeDiscoverer(node1, node2, node3)

	// create test cluster
	cl, err := hasql.NewCluster(discoverer, hasql.PostgreSQLChecker,
		hasql.WithUpdateInterval[*sql.DB](10*time.Millisecond),
	)
	require.NoError(t, err)

	// close cluster and all underlying pools in the end
	defer func() {
		assert.NoError(t, cl.Close())
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// wait for any alive node
	waitNode, err := cl.WaitForNode(ctx, hasql.Alive)
	assert.NoError(t, err)
	assert.Contains(t, []*hasql.Node[*sql.DB]{node1, node2, node3}, waitNode)

	// pick primary node
	primary := cl.Node(hasql.Primary)
	assert.Same(t, node1, primary)

	// pick standby node
	standby := cl.Node(hasql.Standby)
	assert.Contains(t, []*hasql.Node[*sql.DB]{node2, node3}, standby)
}

// TestEnd2End_SingleDeadNodeCluster setups 3 node cluster, waits for at least one
// alive node, then picks primary and secondary node. One node is always dead.
func TestEnd2End_SingleDeadNodeCluster(t *testing.T) {
	// create three database pools
	db1, mock1, err := sqlmock.New()
	require.NoError(t, err)
	db2, mock2, err := sqlmock.New()
	require.NoError(t, err)
	db3, mock3, err := sqlmock.New()
	require.NoError(t, err)

	// set db1 to be primary node
	mock1.ExpectQuery(`SELECT.*pg_is_in_recovery`).
		WillReturnRows(sqlmock.
			NewRows([]string{"role", "lag"}).
			AddRow(1, 0),
		)
	// set db2 to be standby node
	mock2.ExpectQuery(`SELECT.*pg_is_in_recovery`).
		WillReturnRows(sqlmock.
			NewRows([]string{"role", "lag"}).
			AddRow(2, 0),
		)
	// db3 will be always dead
	mock3.ExpectQuery(`SELECT.*pg_is_in_recovery`).
		WillDelayFor(time.Second).
		WillReturnError(io.EOF)

	// all pools must be closed in the end
	mock1.ExpectClose()
	mock2.ExpectClose()
	mock3.ExpectClose()

	// register pools as nodes
	node1 := hasql.NewNode("ololo", db1)
	node2 := hasql.NewNode("trololo", db2)
	node3 := hasql.NewNode("shimba", db3)
	discoverer := hasql.NewStaticNodeDiscoverer(node1, node2, node3)

	// create test cluster.
	cl, err := hasql.NewCluster(discoverer, hasql.PostgreSQLChecker,
		hasql.WithUpdateInterval[*sql.DB](10*time.Millisecond),
		hasql.WithUpdateTimeout[*sql.DB](50*time.Millisecond),
		// set node picker to round robin to guarantee iteration across all nodes
		hasql.WithNodePicker(new(hasql.RoundRobinNodePicker[*sql.DB])),
	)
	require.NoError(t, err)

	// close cluster and all underlying pools in the end
	defer func() {
		assert.NoError(t, cl.Close())
	}()

	// Set context timeout to be greater than cluster update interval and timeout.
	// If we set update timeout to be greater than wait context timeout
	// we will always receive context.DeadlineExceeded error as current cycle of update
	// will try to gather info about dead node (and thus update whole cluster state)
	// longer that we are waiting for node
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// wait for any alive node
	waitNode, err := cl.WaitForNode(ctx, hasql.Alive)
	assert.NoError(t, err)
	assert.Contains(t, []*hasql.Node[*sql.DB]{node1, node2}, waitNode)

	// pick primary node
	primary := cl.Node(hasql.Primary)
	assert.Same(t, node1, primary)

	// pick standby node multiple times to ensure
	// we always get alive standby node
	for range 100 {
		standby := cl.Node(hasql.Standby)
		assert.Same(t, node2, standby)
	}
}

// TestEnd2End_NoPrimaryCluster setups 3 node cluster, waits for at least one
// alive node, then picks primary and secondary node. No alive primary nodes present.
func TestEnd2End_NoPrimaryCluster(t *testing.T) {
	// create three database pools
	db1, mock1, err := sqlmock.New()
	require.NoError(t, err)
	db2, mock2, err := sqlmock.New()
	require.NoError(t, err)
	db3, mock3, err := sqlmock.New()
	require.NoError(t, err)

	// db1 is always dead
	mock1.ExpectQuery(`SELECT.*pg_is_in_recovery`).
		WillReturnError(io.EOF)
	// set db2 to be standby node
	mock2.ExpectQuery(`SELECT.*pg_is_in_recovery`).
		WillReturnRows(sqlmock.
			NewRows([]string{"role", "lag"}).
			AddRow(2, 10),
		)
	// set db3 to be standby node
	mock3.ExpectQuery(`SELECT.*pg_is_in_recovery`).
		WillReturnRows(sqlmock.
			NewRows([]string{"role", "lag"}).
			AddRow(2, 0),
		)

	// all pools must be closed in the end
	mock1.ExpectClose()
	mock2.ExpectClose()
	mock3.ExpectClose()

	// register pools as nodes
	node1 := hasql.NewNode("ololo", db1)
	node2 := hasql.NewNode("trololo", db2)
	node3 := hasql.NewNode("shimba", db3)
	discoverer := hasql.NewStaticNodeDiscoverer(node1, node2, node3)

	// create test cluster.
	cl, err := hasql.NewCluster(discoverer, hasql.PostgreSQLChecker,
		hasql.WithUpdateInterval[*sql.DB](10*time.Millisecond),
	)
	require.NoError(t, err)

	// close cluster and all underlying pools in the end
	defer func() {
		assert.NoError(t, cl.Close())
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// wait for any alive node
	waitNode, err := cl.WaitForNode(ctx, hasql.Alive)
	assert.NoError(t, err)
	assert.Contains(t, []*hasql.Node[*sql.DB]{node2, node3}, waitNode)

	// pick primary node
	primary := cl.Node(hasql.Primary)
	assert.Nil(t, primary)

	// pick standby node
	standby := cl.Node(hasql.Standby)
	assert.Contains(t, []*hasql.Node[*sql.DB]{node2, node3}, standby)
}
