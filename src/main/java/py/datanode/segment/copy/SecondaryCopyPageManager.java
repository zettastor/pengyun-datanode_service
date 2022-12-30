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

package py.datanode.segment.copy;

import java.util.List;
import py.datanode.exception.CopyPageAbortException;
import py.datanode.exception.SnapshotPageInGcException;
import py.datanode.segment.datalog.MutationLogEntry;
import py.proto.Broadcastlog;

public interface SecondaryCopyPageManager extends CopyPageManager {
  long getPrimaryMaxLogId();

  void setPrimaryMaxLogId(long maxLogId);

  MutationLogEntry getCatchUpLog();

  void setCatchUpLog(MutationLogEntry entry);

  boolean moveToNextCopyPageUnit(int workerId);

  boolean markMaxLogId(int pageIndex, long newLogId);

  long getMaxLogId(int pageIndex);

  void buildCurrentCopyPageUnit(Broadcastlog.PbCopyPageResponse.Builder responseBuilder,
      int workerId);

  void buildNextCopyPageUnit(Broadcastlog.PbCopyPageResponse.Builder responseBuilder, int workerId);

  void pageWrittenByNewLogs(int pageIndex);

  CopyPage[] getCopyUnit(int workerId);

  int getCopyUnitPosition(int workerId);

  double progress();

  int getTotalCountOfPageToCopy();

  void removeTask(int workerId);

  void allocatePageAddressAtTheFirstTime(List<Broadcastlog.PbPageRequest> pbPageRequests,
      int workerId) throws CopyPageAbortException, SnapshotPageInGcException;

}
