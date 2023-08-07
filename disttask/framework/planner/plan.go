// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package planner

import (
	"context"
	"github.com/pingcap/tidb/disttask/framework/proto"
)

type plan interface {
	TP() string
	Children() []plan
	ToSubtasks() []*proto.Subtask
}

type DistPlanBuilder struct {
	task *proto.Task
}

func (b *DistPlanBuilder) Build(ctx context.Context) (plan, error) {
	switch b.task.Type {
	case proto.ImportInto:
		return b.buildImportIntoPlan()
	case proto.TaskTypeExample:
		return nil, nil
	}
	return nil, nil
}

func (b *DistPlanBuilder) buildImportIntoPlan() (plan, error) {
	return nil, nil
}

type stage struct {
	plan     plan
	subtasks []*proto.Subtask
}

type DistPlanner struct {
	stages         []*stage
	finishedStages []*stage
}

func (p *DistPlanner) BuildPlan() {
	// generate

	// write to task table
}

func (p *DistPlanner) IsCurrentStageFinished() bool {

}

func (p *DistPlanner) SubmitStage() {

}
