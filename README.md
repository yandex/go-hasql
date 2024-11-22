# hasql

[![PkgGoDev](https://pkg.go.dev/badge/golang.yandex/hasql)](https://pkg.go.dev/golang.yandex/hasql)
[![GoDoc](https://godoc.org/golang.yandex/hasql?status.svg)](https://godoc.org/golang.yandex/hasql)
![tests](https://github.com/yandex/go-hasql/workflows/tests/badge.svg?branch=master)
![lint](https://github.com/yandex/go-hasql/workflows/lint/badge.svg?branch=master)
[![Go Report Card](https://goreportcard.com/badge/golang.yandex/hasql)](https://goreportcard.com/report/golang.yandex/hasql)
[![codecov](https://codecov.io/gh/yandex/go-hasql/branch/master/graph/badge.svg)](https://codecov.io/gh/yandex/go-hasql)

`hasql` provides simple and reliable way to access high-availability database setups with multiple hosts.

## Status
`hasql` is production-ready and is actively used inside Yandex' production environment.

## Prerequisites

- **[Go](https://golang.org)**: any one of the **two latest major** [releases](https://golang.org/doc/devel/release.html).

## Installation

With [Go module](https://github.com/golang/go/wiki/Modules) support, simply add the following import

```go
import "golang.yandex/hasql/v2"
```

to your code, and then `go [build|run|test]` will automatically fetch the
necessary dependencies.

Otherwise, to install the `hasql` package, run the following command:

```console
$ go get -u golang.yandex/hasql/v2
```

## How does it work
`hasql` operates using standard `database/sql` connection pool objects. User creates `*sql.DB`-compatible objects for each node of database cluster and passes them to constructor. Library keeps up to date information on state of each node by 'pinging' them periodically. User is provided with a set of interfaces to retrieve database node object suitable for required operation.

```go
dbFoo, _ := sql.Open("pgx", "host=foo")
dbBar, _ := sql.Open("pgx", "host=bar")

discoverer := NewStaticNodeDiscoverer(
    NewNode("foo", dbFoo),
    NewNode("bar", dbBar),
)

cl, err := hasql.NewCluster(discoverer, hasql.PostgreSQLChecker)
if err != nil { ... }

node := cl.Primary()
if node == nil {
    err := cl.Err() // most recent errors for all nodes in the cluster
}

fmt.Printf("got node %s\n", node)

ctx, cancel := context.WithTimeout(context.Background(), time.Second)
defer cancel()

if err = node.DB().PingContext(ctx); err != nil { ... }
```

`hasql` does not configure provided connection pools in any way. It is user's job to set them up properly. Library does handle their lifetime though - pools are closed when `Cluster` object is closed.

### Concepts and entities

**Cluster** is a set of `database/sql`-compatible nodes that tracks their lifespan and provides access to each individual nodes.

**Node** is a single database instance in high-availability cluster.

**Node discoverer** provides nodes objects to cluster. This abstraction allows user to dynamically change set of cluster nodes, for example collect nodes list via Service Discovery (etcd, Consul).

**Node checker** collects information about current state of individual node in cluster, such as: cluster role, network latency, replication lag, etc. 

**Node picker** picks node from cluster by given criterion using predefined algorithm: random, round-robin, lowest latency, etc.

### Supported criteria
_Alive primary_|_Alive standby_|_Any alive_ node or _none_ otherwise
```go
node := c.Node(hasql.Alive)
```

_Alive primary_ or _none_ otherwise
```go
node := c.Node(hasql.Primary)
```

_Alive standby_ or _none_ otherwise
```go
node := c.Node(hasql.Standby)
```

_Alive primary_|_Alive standby_ or _none_ otherwise
```go
node := c.Node(hasql.PreferPrimary)
```

_Alive standby_|_Alive primary_ or _none_ otherwise
```go
node := c.Node(hasql.PreferStandby)
```

### Node pickers
When user asks `Cluster` object for a node a random one from a list of suitable nodes is returned. User can override this behavior by providing a custom node picker.

Library provides a couple of predefined pickers. For example if user wants 'closest' node (with lowest latency) `LatencyNodePicker` should be used.

```go
cl, err := hasql.NewCluster(
    hasql.NewStaticNodeDiscoverer(hasql.NewNode("foo", dbFoo), hasql.NewNode("bar", dbBar)),
    hasql.PostgreSQLChecker,
    hasql.WithNodePicker(new(hasql.LatencyNodePicker[*sql.DB])),
)
```

## Supported databases
Since library requires `Querier` interface, which describes a subset of `database/sql.DB` methods, it supports any database that has a `database/sql` driver. All it requires is a database-specific checker function that can provide node state info.

Check out `node_checker.go` file for more information.

### Caveats
Node's state is transient at any given time. If `Primary()` returns a node it does not mean that node is still primary when you execute statement on it. All it means is that it was primary when it was last checked. Nodes can change their state at a whim or even go offline and `hasql` can't control it in any manner.

This is one of the reasons why nodes do not expose their perceived state to user.

## Extensions
### Instrumentation
You can add instrumentation via `Tracer` object similar to [httptrace](https://godoc.org/net/http/httptrace) in standard library.
