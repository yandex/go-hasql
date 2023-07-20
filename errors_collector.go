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
	"fmt"
	"sort"
	"sync"
	"time"
)

type nodeError struct {
	addr       string
	err        error
	occurredAt time.Time
}

type errorsCollector struct {
	store map[string]nodeError
	mu    sync.Mutex
}

func newErrorsCollector() *errorsCollector {
	return &errorsCollector{store: make(map[string]nodeError)}
}

func (e *errorsCollector) Add(addr string, err error, occurredAt time.Time) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.store[addr] = nodeError{
		addr:       addr,
		err:        err,
		occurredAt: occurredAt,
	}
}

func (e *errorsCollector) Err() (errs error) {
	errList := make([]nodeError, 0, len(e.store))
	for _, nErr := range e.store {
		errList = append(errList, nErr)
	}
	sort.Slice(errList, func(i, j int) bool {
		return errList[i].occurredAt.Before(errList[j].occurredAt)
	})
	for _, nErr := range errList {
		err := fmt.Errorf("error on node %s occurred at %v: %w", nErr.addr, nErr.occurredAt, nErr.err)
		// use errs = errors.Join(errs, err) when support go<1.20 will be dropped + change to desc errList soring above
		if errs == nil {
			errs = err
		} else {
			errs = fmt.Errorf("%w; %w", err, errs)
		}
	}
	return
}
