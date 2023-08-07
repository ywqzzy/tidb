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
	Child() plan
	ToSubtasks(task *proto.Task) ([][]byte, error)
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
	res := &importPlan2{}
	res.SetChildren(&importPlan1{})
	return res, nil
}

type stage struct {
	plan     plan
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
	task           *proto.Task
	stages         []*stage
	finishedStages []*stage
}

func NewDistPlanner(task *proto.Task) *DistPlanner {
	return &DistPlanner{task, nil, nil}
}

func (p *DistPlanner) BuildPlan(ctx context.Context) error {
	// generate plan.
	builder := DistPlanBuilder{p.task}
	plan, err := builder.Build(ctx)
	if err != nil {
		return err
	}
	// generate stage.
	p.generateStage(plan)
	return nil
}

func (p *DistPlanner) generateStage(plan plan) {
	if plan.Child() != nil {
		p.generateStage(plan.Child())
		p.stages = append(p.stages, &stage{plan, nil})
	} else {
		p.stages = append(p.stages, &stage{plan, nil})
	}
}

func (p *DistPlanner) SubmitStage() ([][]byte, error) {
	// ywq todo
	res, err := p.stages[p.task.Step].GenerateTasks(p.task)
	return res, err
}

func (p *DistPlanner) IsCurrentStageFinished() bool {
	return p.stages[p.task.Step].Finished()
}
