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
	"errors"
	"io"
	"slices"
	"sync/atomic"
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
			AddRow(hasql.NodeRolePrimary, 0),
		)

	// set db2 and db3 to be standby nodes
	mock2.ExpectQuery(`SELECT.*pg_is_in_recovery`).
		WillReturnRows(sqlmock.
			NewRows([]string{"role", "lag"}).
			AddRow(hasql.NodeRoleStandby, 0),
		)
	mock3.ExpectQuery(`SELECT.*pg_is_in_recovery`).
		WillReturnRows(sqlmock.
			NewRows([]string{"role", "lag"}).
			AddRow(hasql.NodeRoleStandby, 10),
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
			AddRow(hasql.NodeRolePrimary, 0),
		)
	// set db2 to be standby node
	mock2.ExpectQuery(`SELECT.*pg_is_in_recovery`).
		WillReturnRows(sqlmock.
			NewRows([]string{"role", "lag"}).
			AddRow(hasql.NodeRoleStandby, 0),
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
			AddRow(hasql.NodeRoleStandby, 10),
		)
	// set db3 to be standby node
	mock3.ExpectQuery(`SELECT.*pg_is_in_recovery`).
		WillReturnRows(sqlmock.
			NewRows([]string{"role", "lag"}).
			AddRow(hasql.NodeRoleStandby, 0),
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

	// cluster must keep last check error
	assert.ErrorIs(t, cl.Err(), io.EOF)
}

// TestEnd2End_DeadCluster setups 3 node cluster. None node is alive.
func TestEnd2End_DeadCluster(t *testing.T) {
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
		WillReturnError(io.EOF)
	// set db3 to be standby node
	mock3.ExpectQuery(`SELECT.*pg_is_in_recovery`).
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
	)
	require.NoError(t, err)

	// close cluster and all underlying pools in the end
	defer func() {
		assert.NoError(t, cl.Close())
	}()

	// set context expiration to be greater than cluster refresh interval and timeout
	// to guarantee at least one cycle of state refresh
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// wait for any alive node
	waitNode, err := cl.WaitForNode(ctx, hasql.Alive)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Nil(t, waitNode)

	// pick primary node
	primary := cl.Node(hasql.Primary)
	assert.Nil(t, primary)

	// pick standby node
	standby := cl.Node(hasql.Standby)
	assert.Nil(t, standby)
}

// TestEnd2End_FlakyCluster setups 3 node cluster, waits for at least one
// alive node, then picks primary and secondary node.
// One node fails to report it's state between refresh intervals.
func TestEnd2End_FlakyCluster(t *testing.T) {
	errIsPrimary := errors.New("primary node")
	errIsStandby := errors.New("standby node")

	sentinelErrChecker := func(ctx context.Context, q hasql.Querier) (hasql.NodeInfoProvider, error) {
		_, err := q.QueryContext(ctx, "report node pls")
		if errors.Is(err, errIsPrimary) {
			return hasql.NodeInfo{ClusterRole: hasql.NodeRolePrimary}, nil
		}
		if errors.Is(err, errIsStandby) {
			return hasql.NodeInfo{ClusterRole: hasql.NodeRoleStandby}, nil
		}
		return nil, err
	}

	// set db1 to be primary node
	// it will fail with error on every second attempt to query state
	var attempts uint32
	db1 := &mockQuerier{
		queryFn: func(_ context.Context, _ string, _ ...any) (*sql.Rows, error) {
			call := atomic.AddUint32(&attempts, 1)
			if call%2 == 0 {
				return nil, io.EOF
			}
			return nil, errIsPrimary
		},
	}

	// set db2 and db3 to be standbys
	db2 := &mockQuerier{
		queryFn: func(_ context.Context, _ string, _ ...any) (*sql.Rows, error) {
			return nil, errIsStandby
		},
	}
	db3 := &mockQuerier{
		queryFn: func(_ context.Context, _ string, _ ...any) (*sql.Rows, error) {
			return nil, errIsStandby
		},
	}

	// register pools as nodes
	node1 := hasql.NewNode("ololo", db1)
	node2 := hasql.NewNode("trololo", db2)
	node3 := hasql.NewNode("shimba", db3)
	discoverer := hasql.NewStaticNodeDiscoverer(node1, node2, node3)

	// create test cluster
	cl, err := hasql.NewCluster(discoverer, sentinelErrChecker,
		hasql.WithUpdateInterval[*mockQuerier](50*time.Millisecond),
	)
	require.NoError(t, err)

	// close cluster and all underlying pools in the end
	defer func() {
		assert.NoError(t, cl.Close())
	}()

	// wait for a long time
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	// wait for any alive node
	waitNode, err := cl.WaitForNode(ctx, hasql.Alive)
	assert.NoError(t, err)
	assert.Contains(t, []*hasql.Node[*mockQuerier]{node1, node2, node3}, waitNode)

	// fetch nodes often
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	var primaryStates []string
	for {
		select {
		case <-ctx.Done():
			// check that primary node has changed its state at least once
			expected := []string{"ololo", "", "ololo", "", "ololo", "", "ololo", "", "ololo", ""}
			assert.Equal(t, expected, slices.Compact(primaryStates))
			// end test
			return
		case <-ticker.C:
			// pick primary node
			primary := cl.Node(hasql.Primary)
			// store current state for further checks
			var name string
			if primary != nil {
				name = primary.String()
			}
			primaryStates = append(primaryStates, name)

			// pick and check standby node
			standby := cl.Node(hasql.Standby)
			assert.Contains(t, []*hasql.Node[*mockQuerier]{node2, node3}, standby)
		}
	}

}

var _ hasql.Querier = (*mockQuerier)(nil)
var _ io.Closer = (*mockQuerier)(nil)

// mockQuerier returns fake SQL results to tests
type mockQuerier struct {
	queryFn    func(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	queryRowFn func(ctx context.Context, query string, args ...any) *sql.Row
	closeFn    func() error
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

func (m *mockQuerier) Close() error {
	if m.closeFn != nil {
		return m.closeFn()
	}
	return nil
}
