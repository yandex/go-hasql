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

type NodeStateCriterion uint8

const (
	// Alive is a criterion to choose any alive node
	Alive NodeStateCriterion = iota + 1
	// Primary is a criterion to choose primary node
	Primary
	// Standby is a criterion to choose standby node
	Standby
	// PreferPrimary is a criterion to choose primary or any alive node
	PreferPrimary
	// PreferStandby is a criterion to choose standby or any alive node
	PreferStandby

	// maxNodeCriterion is for testing purposes only
	// all new criteria must be added above this constant
	maxNodeCriterion
)

type Node[T Querier] struct {
	name string
	db   T
}

// NewNode constructs node with given SQL querier
func NewNode[T Querier](name string, db T) *Node[T] {
	return &Node[T]{name: name, db: db}
}

// DB returns node's database/sql DB
func (n *Node[T]) DB() T {
	return n.db
}

// String implements Stringer.
// It uses name provided at construction to uniquely identify a single node
func (n *Node[T]) String() string {
	return n.name
}
