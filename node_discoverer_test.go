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
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewStaticNodeDiscoverer(t *testing.T) {
	node1 := NewNode("shimba", new(sql.DB))
	node2 := NewNode("boomba", new(sql.DB))

	d := NewStaticNodeDiscoverer(node1, node2)
	expected := StaticNodeDiscoverer[*sql.DB]{
		nodes: []*Node[*sql.DB]{node1, node2},
	}

	assert.Equal(t, expected, d)
}

func TestStaticNodeDiscoverer_DiscoverNodes(t *testing.T) {
	node1 := NewNode("shimba", new(sql.DB))
	node2 := NewNode("boomba", new(sql.DB))

	d := NewStaticNodeDiscoverer(node1, node2)

	discovered, err := d.DiscoverNodes(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, []*Node[*sql.DB]{node1, node2}, discovered)
}
