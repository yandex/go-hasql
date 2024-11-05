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
	"errors"
	"slices"
	"sync"
)

// CheckedNodes holds references to any cluster node which state has been checked
type CheckedNodes[T Querier] struct {
	alive     []CheckedNode[T]
	primaries []CheckedNode[T]
	standbys  []CheckedNode[T]
	err       error
}

// Alive returns a list of all successfully checked nodes irregarding their cluster role
func (c CheckedNodes[T]) Alive() []CheckedNode[T] {
	return c.alive
}

// Primaries returns list of all successfully checked nodes with primary role
func (c CheckedNodes[T]) Primaries() []CheckedNode[T] {
	return c.primaries
}

// Standbys returns list of all successfully checked nodes with standby role
func (c CheckedNodes[T]) Standbys() []CheckedNode[T] {
	return c.standbys
}

// Err holds information about cause of node check failure.
func (c CheckedNodes[T]) Err() error {
	return c.err
}

// CheckedNode contains most recent state of single cluster node
type CheckedNode[T Querier] struct {
	Node *Node[T]
	Info NodeInfoProvider
}

// checkNodes takes slice of nodes, checks them in parallel and returns the alive ones
func checkNodes[T Querier](ctx context.Context, nodes []*Node[T], checkFn NodeChecker, compareFn func(a, b CheckedNode[T]) int, tracer Tracer[T]) CheckedNodes[T] {
	var mu sync.Mutex
	checked := make([]CheckedNode[T], 0, len(nodes))
	var errs NodeCheckErrors[T]

	var wg sync.WaitGroup
	wg.Add(len(nodes))
	for _, node := range nodes {
		go func(node *Node[T]) {
			defer wg.Done()

			// check single node state
			info, err := checkFn(ctx, node.DB())
			if err != nil {
				cerr := NodeCheckError[T]{
					node: node,
					err:  err,
				}

				// node is dead - make trace call
				tracer.NodeDead(cerr)

				// store node check error
				mu.Lock()
				defer mu.Unlock()
				errs = append(errs, cerr)
				return
			}

			cn := CheckedNode[T]{
				Node: node,
				Info: info,
			}

			// make trace call about alive node
			tracer.NodeAlive(cn)

			// store checked alive node
			mu.Lock()
			defer mu.Unlock()
			checked = append(checked, cn)
		}(node)
	}

	// wait for all nodes to be checked
	wg.Wait()

	// sort checked nodes
	slices.SortFunc(checked, compareFn)

	// split checked nodes by roles
	// in almost all cases there is only one master node in cluster
	primaries := make([]CheckedNode[T], 1)
	standbys := make([]CheckedNode[T], 0, len(checked))
	for _, cn := range checked {
		switch cn.Info.Role() {
		case NodeRolePrimary:
			primaries = append(primaries, cn)
		case NodeRoleStandby:
			standbys = append(standbys, cn)
		default:
			// treat node with undetermined role as dead
			cerr := NodeCheckError[T]{
				node: cn.Node,
				err:  errors.New("cannot determine node role"),
			}
			tracer.NodeDead(cerr)
		}
	}

	res := CheckedNodes[T]{
		alive:     checked,
		primaries: make([]CheckedNode[T], 0, 1),
		standbys:  make([]CheckedNode[T], 0, len(checked)),
		err: func() error {
			if len(errs) != 0 {
				return errs
			}
			return nil
		}(),
	}

	return res
}
