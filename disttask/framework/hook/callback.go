// Copyright 2015 PingCAP, Inc.
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

package hook

import (
	"github.com/pingcap/tidb/disttask/framework/storage"
	"github.com/pingcap/tidb/util/logutil"
)

// Callback is used for Dist Task Framework
type Callback interface {
	OnDispatchGTaskBefore(taskMgr *storage.TaskManager)
}

type BaseCallback struct {
}

func (*BaseCallback) OnDispatchGTaskBefore(taskMgr *storage.TaskManager) {
	// Nothing to do
	logutil.BgLogger().Info("ywq test on dispatch gtask before")
	taskMgr.CancelGlobalTask(1)
}
