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
	"io"
	"slices"
)

var _ NodeDiscoverer[*mockQuerier] = (*mockNodeDiscoverer[*mockQuerier])(nil)

// mockNodeDiscoverer returns stored results to tests
type mockNodeDiscoverer[T Querier] struct {
	nodes []*Node[T]
	err   error
}

func (e mockNodeDiscoverer[T]) DiscoverNodes(_ context.Context) ([]*Node[T], error) {
	return slices.Clone(e.nodes), e.err
}

var _ Querier = (*mockQuerier)(nil)
var _ io.Closer = (*mockQuerier)(nil)

// mockQuerier returns fake SQL results to tests
type mockQuerier struct {
	name       string
	queryFn    func(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	queryRowFn func(ctx context.Context, query string, args ...any) *sql.Row
	closeFn    func() error
}

func (m *mockQuerier) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	if m.queryFn != nil {
		return m.queryFn(ctx, query, args...)
	}
	return nil, nil
}

func (m *mockQuerier) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	if m.queryRowFn != nil {
		return m.queryRowFn(ctx, query, args...)
	}
	return nil
}

func (m *mockQuerier) Close() error {
	if m.closeFn != nil {
		return m.closeFn()
	}
	return nil
}
