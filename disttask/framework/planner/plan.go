package planner

import "github.com/pingcap/tidb/disttask/framework/proto"

type Plan interface {
	TP() string
	Child() Plan
	ToSubtasks(task *proto.Task) ([][]byte, error)
}
