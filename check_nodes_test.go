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
	"io"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/gofrs/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCheckedNodesList_Len(t *testing.T) {
	nodes := checkedNodesList{checkedNode{}, checkedNode{}, checkedNode{}}
	require.Equal(t, 3, nodes.Len())
}

func TestCheckedNodesList_Less(t *testing.T) {
	nodes := checkedNodesList{checkedNode{Latency: time.Nanosecond}, checkedNode{Latency: 2 * time.Nanosecond}}
	require.True(t, nodes.Less(0, 1))
	require.False(t, nodes.Less(1, 0))
}

func TestCheckedNodesList_Swap(t *testing.T) {
	nodes := checkedNodesList{checkedNode{Latency: time.Nanosecond}, checkedNode{Latency: 2 * time.Nanosecond}}
	nodes.Swap(0, 1)
	assert.Equal(t, 2*time.Nanosecond, nodes[0].Latency)
	assert.Equal(t, time.Nanosecond, nodes[1].Latency)
}

func TestCheckedNodesList_Sort(t *testing.T) {
	nodes := checkedNodesList{checkedNode{Latency: 2 * time.Nanosecond}, checkedNode{Latency: 3 * time.Nanosecond}, checkedNode{Latency: time.Nanosecond}}
	sort.Sort(nodes)
	for i := range nodes {
		assert.Equal(t, time.Duration(i+1)*time.Nanosecond, nodes[i].Latency)
	}
}

func TestGroupedCheckedNodes_Alive(t *testing.T) {
	// TODO: this test does not cover all the cases but better than nothing
	const count = 10
	var expected []Node
	var input groupedCheckedNodes
	for i := 0; i < count; i++ {
		node := checkedNode{Node: NewNode(fmt.Sprintf("%d", i), nil), Latency: time.Duration(i+1) * time.Nanosecond}
		expected = append(expected, node.Node)
		if i%2 == 0 {
			input.Primaries = append(input.Primaries, node)
		} else {
			input.Standbys = append(input.Standbys, node)
		}
	}
	require.Len(t, expected, count)
	require.NotEmpty(t, input.Primaries)
	require.NotEmpty(t, input.Standbys)
	require.Equal(t, count, len(input.Primaries)+len(input.Standbys))

	alive := input.Alive()
	require.Len(t, alive, count)
	require.Equal(t, expected, alive)
}

func TestCheckNodes(t *testing.T) {
	const count = 100
	var nodes []Node
	expected := AliveNodes{Alive: make([]Node, count)}
	for i := 0; i < count; i++ {
		db, _, err := sqlmock.New()
		require.NoError(t, err)
		require.NotNil(t, db)

		node := NewNode(uuid.Must(uuid.NewV4()).String(), db)

		for {
			// Randomize 'order' (latency)
			pos := rand.Intn(count)
			if expected.Alive[pos] == nil {
				expected.Alive[pos] = node
				break
			}
		}

		nodes = append(nodes, node)
	}

	require.Len(t, expected.Alive, count)

	// Fill primaries and standbys
	for i, node := range expected.Alive {
		if i%2 == 0 {
			expected.Primaries = append(expected.Primaries, node)
		} else {
			expected.Standbys = append(expected.Standbys, node)
		}
	}

	require.NotEmpty(t, expected.Primaries)
	require.NotEmpty(t, expected.Standbys)
	require.Equal(t, count, len(expected.Primaries)+len(expected.Standbys))

	executor := func(ctx context.Context, target *checkedNode) error {
		// Alive nodes set the expected 'order' (latency) of all available nodes.
		// Return duration based on that order.
		for i, alive := range expected.Alive {
			if alive == target.Node {
				target.Latency = time.Duration(i) * time.Nanosecond
				break
			}
		}

		for _, primary := range expected.Primaries {
			if primary == target.Node {
				target.Primary = true
				return nil
			}
		}

		for _, standby := range expected.Standbys {
			if standby == target.Node {
				target.Primary = false
				return nil
			}
		}

		return errors.New("node not found")
	}

	alive := checkNodes(context.Background(), nodes, Tracer{}, executor)
	assert.Equal(t, expected.Primaries, alive.Primaries)
	assert.Equal(t, expected.Standbys, alive.Standbys)
	assert.Equal(t, expected.Alive, alive.Alive)
}

func TestCheckNodes_ReplicationLag(t *testing.T) {
	// prepare test nodes
	aliveNode := func() Node {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		require.NotNil(t, db)

		rows := sqlmock.
			NewRows([]string{"replication_lag"}).
			AddRow(1 * time.Millisecond)

		mock.
			ExpectQuery("SELECT replication_lag").
			WillReturnRows(rows)

		return NewNode(uuid.Must(uuid.NewV4()).String(), db)
	}()

	faultyNode := func() Node {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		require.NotNil(t, db)

		mock.
			ExpectQuery("SELECT replication_lag").
			WillReturnError(io.ErrUnexpectedEOF)

		return NewNode(uuid.Must(uuid.NewV4()).String(), db)
	}()

	slowNode := func() Node {
		db, mock, err := sqlmock.New()
		require.NoError(t, err)
		require.NotNil(t, db)

		rows := sqlmock.
			NewRows([]string{"replication_lag"}).
			AddRow(1 * time.Second)

		mock.
			ExpectQuery("SELECT replication_lag").
			WillReturnRows(rows)

		return NewNode(uuid.Must(uuid.NewV4()).String(), db)
	}()

	nodes := []Node{aliveNode, faultyNode, slowNode}
	expectedNodes := []Node{aliveNode}

	executor := func(ctx context.Context, target *checkedNode) error {
		var lag time.Duration
		err := target.Node.DB().
			QueryRowContext(ctx, "SELECT replication_lag").
			Scan(&lag)

		if err != nil {
			return err
		}

		if lag > 100*time.Millisecond {
			return errors.New("node too slow")
		}

		return nil
	}

	alive := checkNodes(context.Background(), nodes, Tracer{}, executor)
	assert.Equal(t, expectedNodes, alive.Alive)
}
