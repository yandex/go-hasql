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
	"fmt"
	"slices"
	"sync"
)

// CheckedNodes holds references to all available cluster nodes
type CheckedNodes[T Querier] struct {
	discovered []*Node[T]
	alive      []CheckedNode[T]
	primaries  []CheckedNode[T]
	standbys   []CheckedNode[T]
	err        error
}

// Discovered returns a list of nodes discovered in cluster
func (c CheckedNodes[T]) Discovered() []*Node[T] {
	return c.discovered
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
func checkNodes[T Querier](ctx context.Context, discoverer NodeDiscoverer[T], checkFn NodeChecker, compareFn func(a, b CheckedNode[T]) int, tracer Tracer[T]) CheckedNodes[T] {
	discoveredNodes, err := discoverer.DiscoverNodes(ctx)
	if err != nil {
		// error discovering nodes
		return CheckedNodes[T]{
			err: fmt.Errorf("cannot discover cluster nodes: %w", err),
		}
	}

	var mu sync.Mutex
	checked := make([]CheckedNode[T], 0, len(discoveredNodes))
	var errs NodeCheckErrors[T]

	var wg sync.WaitGroup
	wg.Add(len(discoveredNodes))
	for _, node := range discoveredNodes {
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
	// in almost all cases there is only one primary node in cluster
	primaries := make([]CheckedNode[T], 0, 1)
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
		discovered: discoveredNodes,
		alive:      checked,
		primaries:  primaries,
		standbys:   standbys,
		err: func() error {
			if len(errs) != 0 {
				return errs
			}
			return nil
		}(),
	}

	return res
}

// pickNodeByCriterion is a helper function to pick a single node by given criterion
func pickNodeByCriterion[T Querier](nodes CheckedNodes[T], picker NodePicker[T], criterion NodeStateCriterion) *Node[T] {
	var subset []CheckedNode[T]

	switch criterion {
	case Alive:
		subset = nodes.alive
	case Primary:
		subset = nodes.primaries
	case Standby:
		subset = nodes.standbys
	case PreferPrimary:
		if subset = nodes.primaries; len(subset) == 0 {
			subset = nodes.standbys
		}
	case PreferStandby:
		if subset = nodes.standbys; len(subset) == 0 {
			subset = nodes.primaries
		}
	}

	if len(subset) == 0 {
		return nil
	}

	return picker.PickNode(subset).Node
}
