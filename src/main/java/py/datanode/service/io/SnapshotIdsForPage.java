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
