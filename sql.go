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
)

// Querier describes abstract base SQL client such as database/sql.DB.
// Most of database/sql compatible third-party libraries already implement it
type Querier interface {
	// QueryRowContext executes a query that is expected to return at most one row
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	// QueryContext executes a query that returns rows, typically a SELECT
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}
