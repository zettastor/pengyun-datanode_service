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

import java.io.IOException;
import org.apache.commons.lang.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.segment.SegId;
import py.common.struct.Pair;

public class MutationLogEntryReaderCompactImplForPersistedLogs extends
    MutationLogEntryReaderCompactImpl {
  private static final Logger logger = LoggerFactory
      .getLogger(MutationLogEntryReaderCompactImplForPersistedLogs.class);

  @Override
  public MutationLogEntry read() throws IOException {
    super.readInteral();

    if (status != MutationLogEntry.LogStatus.AbortedConfirmed
        && status != MutationLogEntry.LogStatus.Committed) {
      logger.error("persisted log {} {} can't be other status except Committed or AbortedConfirmed",
          logId, status);
      throw new RuntimeException();
    }

    if (eof) {
      return null;
    } else {
      MutationLogEntry logEntry = MutationLogEntryFactory
          .createEmptyLog(uuid, logId, offset, checksum, length);
      logEntry.setStatus(status);
      logEntry.apply();
      logEntry.setPersisted();
      return logEntry;
    }
  }

  @Override
  protected MutationLogEntrySerializationCompactFormat.FieldType[] getFormat() {
    return MutationLogEntrySerializationCompactFormat.fieldsInOrderForPersistedLogs;
  }

  @Override
  public Pair<SegId, MutationLogEntry> readLogAndSegment() throws IOException {
    throw new NotImplementedException("");
  }
}
