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

package py.datanode.segment.datalog;

import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.segment.SegId;
import py.datanode.segment.datalog.MutationLogEntry.LogStatus;
import py.exception.NoAvailableBufferException;

public class MutationLogEntrySaveHelper {
  private static final Logger logger = LoggerFactory.getLogger(MutationLogEntrySaveHelper.class);

  public static MutationLogEntry buildFromSaveLog(MutationLogEntryForSave saveLog)
      throws NoAvailableBufferException {
    Validate.notNull(saveLog);

    MutationLogEntry log = MutationLogEntryFactory
        .createLogForPrimary(saveLog.getUuid(), saveLog.getLogId(), saveLog.getArchiveId(),
            saveLog.getOffset(),
            saveLog.getData(), saveLog.getChecksum());
    log.setStatus(LogStatus.findByValue(saveLog.getStatus()));
    log.setLength(saveLog.getDataLength());
    if (saveLog.isApplied()) {
      log.apply();
    }
    return log;
  }

  public static MutationLogEntryForSave buildFromLog(MutationLogEntry log) {
    Validate.notNull(log);
    MutationLogEntryForSave saveLog = null;
    try {
      saveLog = new MutationLogEntryForSave(log.getUuid(), log.getLogId(), log.getOffset(),
          log.getLength(),
          log.getData(), log.getChecksum(), log.getStatus().name(),
          log.isApplied(), log.isPersisted());
    } catch (Exception e) {
      logger.error("caught an exception", e);
    }
    return saveLog;
  }

  public static String buildSaveLogFileNameWithSegId(SegId segId) {
    return String.valueOf(segId.getVolumeId().getId()) + "_" + String.valueOf(segId.getIndex());
  }
}
