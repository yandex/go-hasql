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
	"math/rand/v2"
	"sync/atomic"
	"time"
)

// NodePicker decides which node must be used from given set.
// It also provides a comparer to be used to pre-sort nodes for better performance
type NodePicker[T Querier] interface {
	// PickNode returns a single node from given set
	PickNode(nodes []CheckedNode[T]) CheckedNode[T]
	// CompareNodes is a comparison function to be used to sort checked nodes
	CompareNodes(a, b CheckedNode[T]) int
}

// RandomNodePicker implements NodePicker.
// It returns random node on each call and does not sort checked nodes
type RandomNodePicker[T Querier] struct{}

func (_ *RandomNodePicker[T]) PickNode(nodes []CheckedNode[T]) CheckedNode[T] {
	return nodes[rand.IntN(len(nodes))]
}

func (_ *RandomNodePicker[T]) CompareNodes(_, _ CheckedNode[T]) int {
	return 0
}

// RoundRobinNodePicker implements NodePicker.
// It returns next node based on Round Robin algorithm and tries to preserve nodes order across checks
type RoundRobinNodePicker[T Querier] struct {
	idx uint32
}

func (r *RoundRobinNodePicker[T]) PickNode(nodes []CheckedNode[T]) CheckedNode[T] {
	n := atomic.AddUint32(&r.idx, 1)
	return nodes[(int(n)-1)%len(nodes)]
}

func (r *RoundRobinNodePicker[T]) CompareNodes(a, b CheckedNode[T]) int {
	aName, bName := a.Node.String(), b.Node.String()
	if aName < bName {
		return -1
	}
	if aName > bName {
		return 1
	}
	return 0
}

// LatencyNodePicker implements NodePicker.
// It returns node with least latency and sorts checked nodes by reported latency ascending.
// WARNING: This picker requires that NodeInfoProvider can report node's network latency otherwise code will panic!
type LatencyNodePicker[T Querier] struct{}

func (_ *LatencyNodePicker[T]) PickNode(nodes []CheckedNode[T]) CheckedNode[T] {
	return nodes[0]
}

func (_ *LatencyNodePicker[T]) CompareNodes(a, b CheckedNode[T]) int {
	aLatency := a.Info.(interface{ Latency() time.Duration }).Latency()
	bLatency := b.Info.(interface{ Latency() time.Duration }).Latency()

	if aLatency < bLatency {
		return -1
	}
	if aLatency > bLatency {
		return 1
	}
	return 0
}

// ReplicationNodePicker implements NodePicker.
// It returns node with smallest replication lag and sorts checked nodes by reported replication lag ascending.
// Note that replication lag reported by checkers can vastly differ from the real situation on standby server.
// WARNING: This picker requires that NodeInfoProvider can report node's replication lag otherwise code will panic!
type ReplicationNodePicker[T Querier] struct{}

func (_ *ReplicationNodePicker[T]) PickNode(nodes []CheckedNode[T]) CheckedNode[T] {
	return nodes[0]
}

func (_ *ReplicationNodePicker[T]) CompareNodes(a, b CheckedNode[T]) int {
	aLag := a.Info.(interface{ ReplicationLag() int }).ReplicationLag()
	bLag := b.Info.(interface{ ReplicationLag() int }).ReplicationLag()

	if aLag < bLag {
		return -1
	}
	if aLag > bLag {
		return 1
	}
	return 0
}