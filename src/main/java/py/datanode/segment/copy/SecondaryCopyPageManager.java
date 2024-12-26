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
