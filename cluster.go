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
	"io"
	"sync"
	"sync/atomic"
	"time"
)

// Cluster consists of number of 'nodes' of a single SQL database.
type Cluster[T Querier] struct {
	// configuration
	updateInterval time.Duration
	updateTimeout  time.Duration
	discoverer     NodeDiscoverer[T]
	checker        NodeChecker
	picker         NodePicker[T]
	tracer         Tracer[T]

	// status
	checkedNodes atomic.Value
	stop         context.CancelFunc

	// broadcast
	subscribersMu sync.Mutex
	subscribers   []updateSubscriber[T]
}

// NewCluster returns object representing a single 'cluster' of SQL databases
func NewCluster[T Querier](discoverer NodeDiscoverer[T], checker NodeChecker, opts ...ClusterOpt[T]) (*Cluster[T], error) {
	if discoverer == nil {
		return nil, errors.New("node discoverer required")
	}

	// prepare internal 'stop' context
	ctx, stopFn := context.WithCancel(context.Background())

	cl := &Cluster[T]{
		updateInterval: 5 * time.Second,
		updateTimeout:  time.Second,
		discoverer:     discoverer,
		checker:        checker,
		picker:         new(RandomNodePicker[T]),
		tracer:         BaseTracer[T]{},

		stop: stopFn,
	}

	// apply options
	for _, opt := range opts {
		opt(cl)
	}

	// Store initial nodes state
	cl.checkedNodes.Store(CheckedNodes[T]{})

	// Start update routine
	go cl.backgroundNodesUpdate(ctx)
	return cl, nil
}

// Close stops node updates.
// Close function must be called when cluster is not needed anymore.
// It returns combined error if multiple nodes returned errors
func (cl *Cluster[T]) Close() (err error) {
	cl.stop()

	// close all nodes underlying connection pools
	discovered := cl.checkedNodes.Load().(CheckedNodes[T]).discovered
	for _, node := range discovered {
		if closer, ok := any(node.DB()).(io.Closer); ok {
			err = errors.Join(err, closer.Close())
		}
	}

	// discard any collected state of nodes
	cl.checkedNodes.Store(CheckedNodes[T]{})

	return err
}

// Err returns cause of nodes most recent check failures.
// In most cases error is a list of errors of type CheckNodeErrors, original errors
// could be extracted using `errors.As`.
// Example:
//
//	var cerrs NodeCheckErrors
//	if errors.As(err, &cerrs) {
//	    for _, cerr := range cerrs {
//	        fmt.Printf("node: %s, err: %s\n", cerr.Node(), cerr.Err())
//	    }
//	}
func (cl *Cluster[T]) Err() error {
	return cl.checkedNodes.Load().(CheckedNodes[T]).Err()
}

// Node returns cluster node with specified status
func (cl *Cluster[T]) Node(criterion NodeStateCriterion) *Node[T] {
	return pickNodeByCriterion(cl.checkedNodes.Load().(CheckedNodes[T]), cl.picker, criterion)
}

// WaitForNode with specified status to appear or until context is canceled
func (cl *Cluster[T]) WaitForNode(ctx context.Context, criterion NodeStateCriterion) (*Node[T], error) {
	// Node already exists?
	node := cl.Node(criterion)
	if node != nil {
		return node, nil
	}

	ch := cl.addUpdateSubscriber(criterion)

	// Node might have appeared while we were adding waiter, recheck
	node = cl.Node(criterion)
	if node != nil {
		return node, nil
	}

	// If channel is unbuffered and we are right here when nodes are updated,
	// the update code won't be able to write into channel and will 'forget' it.
	// Then we will report nil to the caller, either because update code
	// closes channel or because context is canceled.
	//
	// In both cases its not what user wants.
	//
	// We can solve it by doing cl.Node(ns) if/when we are about to return nil.
	// But if another update runs between channel read and cl.Node(ns) AND no
	// nodes have requested status, we will still return nil.
	//
	// Also code becomes more complex.
	//
	// Wait for node to appear...
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case node := <-ch:
		return node, nil
	}
}

// backgroundNodesUpdate periodically checks list of registered nodes
func (cl *Cluster[T]) backgroundNodesUpdate(ctx context.Context) {
	// initial update
	cl.updateNodes(ctx)

	ticker := time.NewTicker(cl.updateInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cl.updateNodes(ctx)
		}
	}
}

// updateNodes checks all nodes and notifies all subscribers
func (cl *Cluster[T]) updateNodes(ctx context.Context) {
	cl.tracer.UpdateNodes()

	ctx, cancel := context.WithTimeout(ctx, cl.updateTimeout)
	defer cancel()

	checked := checkNodes(ctx, cl.discoverer, cl.checker, cl.picker.CompareNodes, cl.tracer)
	cl.checkedNodes.Store(checked)

	cl.tracer.UpdatedNodes(checked)

	cl.notifyUpdateSubscribers(checked)

	cl.tracer.NotifiedWaiters()
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
