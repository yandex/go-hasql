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

import "golang.yandex/hasql"

//nolint:golint
// Forwarded types
type (
	ClusterOption     = hasql.ClusterOption
	NodeStateCriteria = hasql.NodeStateCriteria
	NodeChecker       = hasql.NodeChecker
	NodePicker        = hasql.NodePicker
	AliveNodes        = hasql.AliveNodes
	Tracer            = hasql.Tracer
)

// Forwarded variables and functions
var (
	Alive         = hasql.Alive
	Primary       = hasql.Primary
	Standby       = hasql.Standby
	PreferPrimary = hasql.PreferPrimary
	PreferStandby = hasql.PreferStandby

	WithUpdateInterval = hasql.WithUpdateInterval
	WithUpdateTimeout  = hasql.WithUpdateTimeout
	WithNodePicker     = hasql.WithNodePicker
	WithTracer         = hasql.WithTracer

	PickNodeRandom     = hasql.PickNodeRandom
	PickNodeRoundRobin = hasql.PickNodeRoundRobin
)
