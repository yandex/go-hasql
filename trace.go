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

// Tracer is a set of hooks to be called at various stages of background nodes status update
type Tracer[T Querier] interface {
	// UpdateNodes is called when before updating nodes status.
	UpdateNodes()
	// UpdatedNodes is called after all nodes are updated. The nodes is a list of currently alive nodes.
	UpdatedNodes(nodes CheckedNodes[T])
	// NodeDead is called when it is determined that specified node is dead.
	NodeDead(NodeCheckError[T])
	// NodeAlive is called when it is determined that specified node is alive.
	NodeAlive(node CheckedNode[T])
	// NotifiedWaiters is called when all callers of 'WaitFor*' functions have been notified.
	NotifiedWaiters()
}

// Base tracer is a customizable embedable tracer implementation.
// By default it is a no-op tracer
type BaseTracer[T Querier] struct {
	UpdateNodesFn     func()
	UpdatedNodesFn    func(CheckedNodes[T])
	NodeDeadFn        func(NodeCheckError[T])
	NodeAliveFn       func(CheckedNode[T])
	NotifiedWaitersFn func()
}

func (n BaseTracer[T]) UpdateNodes() {
	if n.UpdateNodesFn != nil {
		n.UpdateNodesFn()
	}
}

func (n BaseTracer[T]) UpdatedNodes(nodes CheckedNodes[T]) {
	if n.UpdatedNodesFn != nil {
		n.UpdatedNodesFn(nodes)
	}
}

func (n BaseTracer[T]) NodeDead(err NodeCheckError[T]) {
	if n.NodeDeadFn != nil {
		n.NodeDeadFn(err)
	}
}

func (n BaseTracer[T]) NodeAlive(node CheckedNode[T]) {
	if n.NodeAliveFn != nil {
		n.NodeAliveFn(node)
	}
}

func (n BaseTracer[T]) NotifiedWaiters() {
	if n.NotifiedWaitersFn != nil {
		n.NotifiedWaitersFn()
	}
}
