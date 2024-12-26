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

package py.datanode.service.io;

import java.util.Objects;

public class SnapshotIdsForPage {
  private boolean noLogExistsInMemory;
  private Integer toReadSnapshotId;
  private Integer maxSnapshotIdForExistingLogs;
  private Integer maxSnapshotIdForExistingAppliedLogs;
  private Integer maxSnapshotIdFromIndexer;
  private Integer segmentMaxAppliedSnapshotId;

  public boolean isMaxSnapshotIdForExistingLogsEqualsToMaxSnapshotIdFromIndexer() {
    return Objects.equals(maxSnapshotIdForExistingLogs, maxSnapshotIdFromIndexer);
  }

  public void setMaxSnapshotIdForExistingLogs(Integer maxSnapshotIdForExistingLogs) {
    this.maxSnapshotIdForExistingLogs = maxSnapshotIdForExistingLogs;
  }

  public void setMaxSnapshotIdForExistingAppliedLogs(Integer maxSnapshotIdForExistingAppliedLogs) {
    this.maxSnapshotIdForExistingAppliedLogs = maxSnapshotIdForExistingAppliedLogs;
  }

  public void setMaxSnapshotIdFromIndexer(Integer maxSnapshotIdFromIndexer) {
    this.maxSnapshotIdFromIndexer = maxSnapshotIdFromIndexer;
  }

  public void setToReadSnapshotId(Integer toReadSnapshotId) {
    this.toReadSnapshotId = toReadSnapshotId;
  }

  public void setSegmentMaxAppliedSnapshotId(Integer segmentMaxAppliedSnapshotId) {
    this.segmentMaxAppliedSnapshotId = segmentMaxAppliedSnapshotId;
  }

  public boolean isNoLogExistsInMemory() {
    return noLogExistsInMemory;
  }

  public void setNoLogExistsInMemory(boolean noLogExistsInMemory) {
    this.noLogExistsInMemory = noLogExistsInMemory;
  }

  @Override
  public String toString() {
    return "SnapshotIdsForPage{"
        + "noLogExistsInMemory=" + noLogExistsInMemory
        + ", toReadSnapshotId=" + toReadSnapshotId
        + ", maxSnapshotIdForExistingLogs=" + maxSnapshotIdForExistingLogs
        + ", maxSnapshotIdForExistingAppliedLogs=" + maxSnapshotIdForExistingAppliedLogs
        + ", maxSnapshotIdFromIndexer=" + maxSnapshotIdFromIndexer
        + ", segmentMaxAppliedSnapshotId=" + segmentMaxAppliedSnapshotId
        + '}';
  }

}
