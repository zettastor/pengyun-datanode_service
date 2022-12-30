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

import py.datanode.segment.copy.bitmap.CopyPageBitmap;
import py.datanode.service.io.throttle.IoThrottleManager;
import py.instance.Instance;

public interface CopyPageManager {
  int getCopyUnitSize();

  CopyPageStatus getCopyPageStatus();

  void setCopyPageStatus(CopyPageStatus newStatus);

  Instance getPeer();

  boolean isFullCopy();

  void setFullCopy(boolean fullCopy);

  long getSessionId();

  boolean markMaxLogId(int pageIndex, long newLogId);

  boolean isDone();

  boolean isProcessing(int pageIndex);

  boolean isProcessed(int pageIndex);

  CopyPageBitmap getCopyPageBitmap();

  CopyPageBitmap getCopyUnitBitmap(int workerId);

  CopyPage[] getCopyUnit(int workerId);

  int getCopyUnitPosition(int workerId);

  long getLastPushTime();

  void setLastPushTime(long lastPushTime);

  IoThrottleManager getIoThrottleManager();

  void setIoThrottleManager(IoThrottleManager ioThrottleManager);

  void removeTask(int workerId);

  int workerCount();

}
