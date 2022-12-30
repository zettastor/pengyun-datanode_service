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
