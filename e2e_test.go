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
	primaryRows := sqlmock.NewRows([]string{"role", "lag"}).AddRow(0, 0)
	mock1.ExpectQuery(`SELECT.*pg_is_in_recovery`).WillReturnRows(primaryRows)

	// set db2 and db3 to be standby nodes
	standbyRows := sqlmock.NewRows([]string{"role", "lag"}).AddRow(1, 0)
	mock2.ExpectQuery(`SELECT.*pg_is_in_recovery`).WillReturnRows(standbyRows)
	mock3.ExpectQuery(`SELECT.*pg_is_in_recovery`).WillReturnRows(standbyRows)

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
}
