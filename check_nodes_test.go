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
	"errors"
	"math/rand"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/gofrs/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCheckNodes(t *testing.T) {
	const count = 10
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
	for _, node := range expected.Alive {
		if rand.Intn(2) == 0 {
			expected.Primaries = append(expected.Primaries, node)
		} else {
			expected.Standbys = append(expected.Standbys, node)
		}
	}

	require.NotEmpty(t, expected.Primaries)
	require.NotEmpty(t, expected.Standbys)
	require.Equal(t, count, len(expected.Primaries)+len(expected.Standbys))

	checker := func(_ context.Context, db *sql.DB) (bool, error) {
		for i, node := range expected.Alive {
			if node.DB() == db {
				// TODO: make test time-independent
				time.Sleep(100 * time.Duration(i) * time.Millisecond)
			}
		}

		for _, node := range expected.Primaries {
			if node.DB() == db {
				return true, nil
			}
		}

		for _, node := range expected.Standbys {
			if node.DB() == db {
				return false, nil
			}
		}

		return false, errors.New("node not found")
	}

	alive := checkNodes(context.Background(), nodes, checker, Tracer{})
	assert.Equal(t, expected.Primaries, alive.Primaries)
	assert.Equal(t, expected.Standbys, alive.Standbys)
	assert.Equal(t, expected.Alive, alive.Alive)
}
