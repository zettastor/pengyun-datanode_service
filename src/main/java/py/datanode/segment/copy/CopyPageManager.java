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
