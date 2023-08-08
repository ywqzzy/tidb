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
	"github.com/pingcap/tidb/util/syncutil"
)

type DistPlanBuilder interface {
	BuildPlan(ctx context.Context) (Plan, error)
}

var DistPlanBuilderMap struct {
	syncutil.RWMutex
	builderMap map[string]DistPlanBuilder
}

// RegisterTaskFlowHandle is used to register the global task handle.
func RegisterDistPlanBuilder(taskType string, builder DistPlanBuilder) {
	DistPlanBuilderMap.Lock()
	DistPlanBuilderMap.builderMap[taskType] = builder
	DistPlanBuilderMap.Unlock()
}

// GetTaskFlowHandle is used to get the global task handle.
func GetDistPlanBuilder(taskType string) DistPlanBuilder {
	DistPlanBuilderMap.Lock()
	defer DistPlanBuilderMap.Unlock()
	return DistPlanBuilderMap.builderMap[taskType]
}

type stage struct {
	plan     Plan
	subtasks []*proto.Subtask
}

func (s *stage) GenerateTasks(task *proto.Task) ([][]byte, error) {
	bytes, err := s.plan.ToSubtasks(task)
	return bytes, err
}

func (s *stage) Finished() bool {
	return true
}

type DistPlanner struct {
	task            *proto.Task
	stages          []*stage
	currentStageIdx int
	finishedStages  []*stage
}

func NewDistPlanner(task *proto.Task) *DistPlanner {
	return &DistPlanner{task, nil, 0, nil}
}

func (p *DistPlanner) BuildPlan(ctx context.Context) error {
	// generate plan.
	builder := GetDistPlanBuilder(p.task.Type)
	plan, err := builder.BuildPlan(ctx)
	if err != nil {
		return err
	}
	// generate stage.
	p.generateStage(plan)
	return nil
}

func (p *DistPlanner) generateStage(plan Plan) {
	if plan.Child() != nil {
		p.generateStage(plan.Child())
		p.stages = append(p.stages, &stage{plan, nil})
	} else {
		p.stages = append(p.stages, &stage{plan, nil})
	}
}

func (p *DistPlanner) SubmitStage() ([][]byte, error) {
	// ywq todo
	res, err := p.stages[p.currentStageIdx].GenerateTasks(p.task)
	p.currentStageIdx++
	return res, err
}

func (p *DistPlanner) IsCurrentStageFinished() bool {
	return p.stages[p.task.Step].Finished()
}

func init() {
	DistPlanBuilderMap.builderMap = make(map[string]DistPlanBuilder)
}
