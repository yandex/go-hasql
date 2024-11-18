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

// Tracer is a set of hooks to be called at various stages of background nodes status update.
// Any particular hook may be nil. Functions may be called concurrently from different goroutines.
type Tracer[T Querier] struct {
	// UpdateNodes is called when before updating nodes status.
	UpdateNodes func()
	// NodesUpdated is called after all nodes are updated. The nodes is a list of currently alive nodes.
	NodesUpdated func(nodes CheckedNodes[T])
	// NodeDead is called when it is determined that specified node is dead.
	NodeDead func(err error)
	// NodeAlive is called when it is determined that specified node is alive.
	NodeAlive func(node CheckedNode[T])
	// WaitersNotified is called when callers of 'WaitForNode' function have been notified.
	WaitersNotified func()
}
