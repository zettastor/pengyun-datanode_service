/*
 * Copyright (c) 2022. PengYunNetWork
 *
 * This program is free software: you can use, redistribute, and/or modify it
 * under the terms of the GNU Affero General Public License, version 3 or later ("AGPL"),
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 *  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *
 *  You should have received a copy of the GNU Affero General Public License along with
 *  this program. If not, see <http://www.gnu.org/licenses/>.
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
