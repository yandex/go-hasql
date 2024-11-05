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

// updateSubscriber represents a waiter for newly checked node event
type updateSubscriber[T Querier] struct {
	ch       chan *Node[T]
	criteria NodeStateCriteria
}

// addUpdateSubscriber adds new dubscriber to notification pool
func (cl *Cluster[T]) addUpdateSubscriber(criteria NodeStateCriteria) <-chan *Node[T] {
	// buffered channel is essential
	// read WaitForNode function for more information
	ch := make(chan *Node[T], 1)
	cl.subscribersMu.Lock()
	defer cl.subscribersMu.Unlock()
	cl.subscribers = append(cl.subscribers, updateSubscriber[T]{ch: ch, criteria: criteria})
	return ch
}

// notifyUpdateSubscribers sends appropriate nodes to registered subsribers.
// This function uses newly checked nodes to avoid race conditions
func (cl *Cluster[T]) notifyUpdateSubscribers(nodes CheckedNodes[T]) {
	cl.subscribersMu.Lock()
	defer cl.subscribersMu.Unlock()

	if len(cl.subscribers) == 0 {
		return
	}

	var nodelessWaiters []updateSubscriber[T]
	// Notify all waiters
	for _, subscriber := range cl.subscribers {
		node := pickNodeByCriteria(nodes, cl.picker, subscriber.criteria)
		if node == nil {
			// Put waiter back
			nodelessWaiters = append(nodelessWaiters, subscriber)
			continue
		}

		// We won't block here, read addUpdateWaiter function for more information
		subscriber.ch <- node
		// No need to close channel since we write only once and forget it so does the 'client'
	}

	cl.subscribers = nodelessWaiters
}
