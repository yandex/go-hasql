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
	"math"
	"time"
)

// NodeRole represents role of node in SQL cluster (usually primary/standby)
type NodeRole uint8

const (
	NodeRoleUnknown NodeRole = iota
	NodeRolePrimary
	NodeRoleStandby
)

// NodeInfoProvider information about single cluster node
type NodeInfoProvider interface {
	// Role reports role of node in cluster.
	// For SQL servers it is usually either primary or standby
	Role() NodeRole
}

var _ NodeInfoProvider = NodeInfo{}

// NodeInfo contains various information about single cluster node.
// It implements NodeInfoProvider with additional useful information
type NodeInfo struct {
	// Role contains determined node's role in cluster
	ClusterRole NodeRole
	// Latency stores time that has been spent to send check request
	// and receive response from server
	NetworkLatency time.Duration
	// ReplicaLag represents how far behind is data on standby
	// in comparison to primary. As determination of real replication
	// lag is a tricky task and value type vary from one DBMS to another
	// (e.g. bytes count lag, time delta lag etc.) this field contains
	// abstract value for sorting purposes only
	ReplicaLag int
}

// Role reports determined role of node in cluster
func (n NodeInfo) Role() NodeRole {
	return n.ClusterRole
}

// Latency reports time spend on query execution from client's point of view.
// It can be used in LatencyNodePicker to determine node with fastest response time
func (n NodeInfo) Latency() time.Duration {
	return n.NetworkLatency
}

// ReplicationLag reports data replication delta on standby.
// It can be used in ReplicationNodePicker to determine node with most up-to-date data
func (n NodeInfo) ReplicationLag() int {
	return n.ReplicaLag
}

// NodeChecker is a function that can perform request to SQL node and retreive various information
type NodeChecker func(context.Context, Querier) (NodeInfoProvider, error)

// PostgreSQLChecker checks state on PostgreSQL node.
// It reports appropriate information for PostgreSQL nodes version 10 and higher
func PostgreSQLChecker(ctx context.Context, db Querier) (NodeInfoProvider, error) {
	start := time.Now()

	var role NodeRole
	var replicationLag *int
	err := db.
		QueryRowContext(ctx, `
			SELECT
				((pg_is_in_recovery())::int + 1) AS role,
				pg_last_wal_receive_lsn() - pg_last_wal_replay_lsn() AS replication_lag
			;
		`).
		Scan(&role, &replicationLag)
	if err != nil {
		return nil, err
	}

	latency := time.Since(start)

	// determine proper replication lag value
	// by default we assume that replication is not started - hence maximum int value
	// see: https://www.postgresql.org/docs/current/functions-admin.html#FUNCTIONS-RECOVERY-CONTROL
	lag := math.MaxInt
	if replicationLag != nil {
		// use reported non-null replication lag
		lag = *replicationLag
	}
	if role == NodeRolePrimary {
		// primary node has no replication lag
		lag = 0
	}

	return NodeInfo{
		ClusterRole:    role,
		NetworkLatency: latency,
		ReplicaLag:     lag,
	}, nil
}

// MySQL checks state of MySQL node.
// ATTENTION: database user must have REPLICATION CLIENT privilege to perform underlying query.
func MySQLChecker(ctx context.Context, db Querier) (NodeInfoProvider, error) {
	start := time.Now()

	rows, err := db.QueryContext(ctx, "SHOW SLAVE STATUS")
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	latency := time.Since(start)

	// only standby MySQL server will return rows for `SHOW SLAVE STATUS` query
	isStandby := rows.Next()
	// TODO: check SECONDS_BEHIND_MASTER row for "replication lag"

	if err := rows.Err(); err != nil {
		return nil, err
	}

	role := NodeRoleStandby
	lag := math.MaxInt
	if !isStandby {
		role = NodeRolePrimary
		lag = 0
	}

	return NodeInfo{
		ClusterRole:    role,
		NetworkLatency: latency,
		ReplicaLag:     lag,
	}, nil
}

// MSSQL checks state of MSSQL node
func MSSQLChecker(ctx context.Context, db Querier) (NodeInfoProvider, error) {
	start := time.Now()

	var isPrimary bool
	err := db.
		QueryRowContext(ctx, `
			SELECT
				IIF(count(database_guid) = 0, 'TRUE', 'FALSE') AS STATUS
			FROM sys.database_recovery_status
				WHERE database_guid IS NULL
		`).
		Scan(&isPrimary)
	if err != nil {
		return nil, err
	}

	latency := time.Since(start)
	role := NodeRoleStandby
	// TODO: proper replication lag calculation
	lag := math.MaxInt
	if isPrimary {
		role = NodeRolePrimary
		lag = 0
	}

	return NodeInfo{
		ClusterRole:    role,
		NetworkLatency: latency,
		ReplicaLag:     lag,
	}, nil
}
