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
	"strings"
)

// NodeCheckErrors is a set of checked nodes errors.
// This type can be used in errors.As/Is as it implements errors.Unwrap method
type NodeCheckErrors[T Querier] []NodeCheckError[T]

func (n NodeCheckErrors[T]) Error() string {
	var b strings.Builder
	for i, err := range n {
		if i > 0 {
			b.WriteByte('\n')
		}
		b.WriteString(err.Error())
	}
	return b.String()
}

// Unwrap is a helper for errors.Is/errors.As functions
func (n NodeCheckErrors[T]) Unwrap() []error {
	errs := make([]error, len(n))
	for i, err := range n {
		errs[i] = err
	}
	return errs
}

// NodeCheckError implements `error` and contains information about unsuccessful node check
type NodeCheckError[T Querier] struct {
	node *Node[T]
	err  error
}

// Node returns dead node instance
func (n NodeCheckError[T]) Node() *Node[T] {
	return n.node
}

// Error implements `error` interface
func (n NodeCheckError[T]) Error() string {
	if n.err == nil {
		return ""
	}
	return n.err.Error()
}

// Unwrap returns underlying error
func (n NodeCheckError[T]) Unwrap() error {
	return n.err
}
