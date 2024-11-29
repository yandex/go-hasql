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
	"time"

	"golang.yandex/hasql/v2"
)

// ExampleCluster shows how to setup basic hasql cluster with some suctom settings
func ExampleCluster() {
	// open connections to database instances
	db1, err := sql.Open("pgx", "host1.example.com")
	if err != nil {
		panic(err)
	}
	db2, err := sql.Open("pgx", "host2.example.com")
	if err != nil {
		panic(err)
	}

	// register connections as nodes with some additional information
	nodes := []*hasql.Node[*sql.DB]{
		hasql.NewNode("bear", db1),
		hasql.NewNode("battlestar galactica", db2),
	}

	// create NodeDiscoverer instance
	// here we use built-in StaticNodeDiscoverer which always returns all registered nodes
	discoverer := hasql.NewStaticNodeDiscoverer(nodes...)
	// use checker suitable for your database
	checker := hasql.PostgreSQLChecker
	// change default RandomNodePicker to RoundRobinNodePicker here
	picker := new(hasql.RoundRobinNodePicker[*sql.DB])

	// create cluster instance using previously created discoverer, checker
	// and some additional options
	cl, err := hasql.NewCluster(discoverer, checker,
		// set custom picker via funcopt
		hasql.WithNodePicker(picker),
		// change interval of cluster state check
		hasql.WithUpdateInterval[*sql.DB](500*time.Millisecond),
		// change cluster check timeout value
		hasql.WithUpdateTimeout[*sql.DB](time.Second),
	)
	if err != nil {
		panic(err)
	}

	// create context with timeout to wait for at least one alive node in cluster
	// note that context timeout value must be greater than cluster update interval + update timeout
	// otherwise you will always receive `context.DeadlineExceeded` error if one of cluster node is dead on startup
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// wait for any alive node to guarantee that your application
	// does not start without database dependency
	// this step usually performed on application startup
	node, err := cl.WaitForNode(ctx, hasql.Alive)
	if err != nil {
		panic(err)
	}

	// pick standby node to perform query
	// always check node for nilness to avoid nil pointer dereference error
	// node object can be nil if no alive nodes for given criterion has been found
	node = cl.Node(hasql.Standby)
	if node == nil {
		panic("no alive standby available")
	}

	// get connection from node and perform desired action
	if err := node.DB().PingContext(ctx); err != nil {
		panic(err)
	}
}
