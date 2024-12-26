/**
* Copyright (C) 2013-2024 Nanjing Pengyun Network Technology Co., Ltd.
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/ 

package py.datanode.segment.datalog.plal.engine;

import com.google.common.collect.RangeSet;
import java.util.List;
import py.datanode.segment.datalog.MutationLogEntry;

public interface PlansToApplyLogs {
  WhatToDo getWhatToDo();

  List<? extends PlanToApplyLog> getPlans();

  String toString();

  boolean isMigrating();

  void canApplyWhenMigrating(boolean migrating);

  WhatToSave getWhatToSave();

  void setWhatToSave(WhatToSave whatToSave);

  default ChunkLogsCollection getChunkLogsCollection() {
    return null;
  }

  default long getMaxLogId() {
    return 0;
  }

  enum WhatToDo {
    Nothing,
    NoNeedToLoadPage,
    LoadPage
  }

  enum WhatToSave {
    ROW,

    Nothing,
  }

  interface PlanToApplyLog {
    MutationLogEntry getLog();

    RangeSet<Integer> getRangesToApply();
  }
}
