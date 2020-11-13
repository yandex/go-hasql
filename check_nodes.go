package hasql

import (
	"context"
	"sort"
	"sync"
	"time"
)

type checkedNode struct {
	Node    Node
	Latency time.Duration
}

type checkedNodesList []checkedNode

var _ sort.Interface = checkedNodesList{}

func (list checkedNodesList) Len() int {
	return len(list)
}

func (list checkedNodesList) Less(i, j int) bool {
	return list[i].Latency < list[j].Latency
}

func (list checkedNodesList) Swap(i, j int) {
	list[i], list[j] = list[j], list[i]
}

func (list checkedNodesList) Nodes() []Node {
	res := make([]Node, 0, len(list))
	for _, node := range list {
		res = append(res, node.Node)
	}

	return res
}

type groupedCheckedNodes struct {
	Primaries checkedNodesList
	Standbys  checkedNodesList
}

func (nodes groupedCheckedNodes) Alive() []Node {
	res := make([]Node, len(nodes.Primaries)+len(nodes.Standbys))

	var i int
	for len(nodes.Primaries) > 0 && len(nodes.Standbys) > 0 {
		if nodes.Primaries[0].Latency < nodes.Standbys[0].Latency {
			res[i] = nodes.Primaries[0].Node
			nodes.Primaries = nodes.Primaries[1:]
		} else {
			res[i] = nodes.Standbys[0].Node
			nodes.Standbys = nodes.Standbys[1:]
		}

		i++
	}

	for j := 0; j < len(nodes.Primaries); j++ {
		res[i] = nodes.Primaries[j].Node
		i++
	}

	for j := 0; j < len(nodes.Standbys); j++ {
		res[i] = nodes.Standbys[j].Node
		i++
	}

	return res
}

func checkNodes(ctx context.Context, nodes []Node, checker NodeChecker, tracer Tracer) AliveNodes {
	checkedNodes := groupedCheckedNodes{
		Primaries: make(checkedNodesList, 0, len(nodes)),
		Standbys:  make(checkedNodesList, 0, len(nodes)),
	}

	var mu sync.Mutex
	var wg sync.WaitGroup
	wg.Add(len(nodes))
	for _, node := range nodes {
		go func(node Node, wg *sync.WaitGroup) {
			defer wg.Done()

			ts := time.Now()
			primary, err := checkNode(ctx, node, checker)
			d := time.Since(ts)
			if err != nil {
				if tracer.NodeDead != nil {
					tracer.NodeDead(node, err)
				}

				return
			}

			if tracer.NodeAlive != nil {
				tracer.NodeAlive(node)
			}

			nl := checkedNode{Node: node, Latency: d}

			mu.Lock()
			defer mu.Unlock()
			if primary {
				checkedNodes.Primaries = append(checkedNodes.Primaries, nl)
			} else {
				checkedNodes.Standbys = append(checkedNodes.Standbys, nl)
			}
		}(node, &wg)
	}
	wg.Wait()

	sort.Sort(checkedNodes.Primaries)
	sort.Sort(checkedNodes.Standbys)

	return AliveNodes{
		Alive:     checkedNodes.Alive(),
		Primaries: checkedNodes.Primaries.Nodes(),
		Standbys:  checkedNodes.Standbys.Nodes(),
	}
}
