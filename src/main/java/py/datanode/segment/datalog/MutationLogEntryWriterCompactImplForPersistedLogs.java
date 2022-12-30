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
import java.nio.ByteBuffer;
import py.archive.segment.SegId;

public class MutationLogEntryWriterCompactImplForPersistedLogs extends
    MutationLogEntryWriterCompactImpl {
  private final int logSize;
  private ByteBuffer cacheBuffer;

  public MutationLogEntryWriterCompactImplForPersistedLogs() {
    logSize = MutationLogEntrySerializationCompactFormat.SERIALIZED_LOG_SIZE_FOR_PERSISTED_LOGS;
    cacheBuffer = ByteBuffer.allocate(logSize);
  }

  public MutationLogEntryWriterCompactImplForPersistedLogs(ByteBuffer cacheBuffer) {
    logSize = MutationLogEntrySerializationCompactFormat.SERIALIZED_LOG_SIZE_FOR_PERSISTED_LOGS;
    this.cacheBuffer = cacheBuffer;
  }

  @Override
  public int write(MutationLogEntry log) throws IOException {
    return writeLogHeader(null, log);
  }

  @Override
  public int write(SegId segId, MutationLogEntry log) throws IOException {
    throw new RuntimeException("not implemented method");
  }

  @Override
  protected ByteBuffer putLogHeaders(SegId segId, MutationLogEntry log) {
    if (!MutationLogEntry.isFinalStatus(log.getStatus())) {
      throw new RuntimeException("log's status is not the final status" + log);
    }

    if (!log.isApplied()) {
      throw new RuntimeException("wrong log status: log is not applied yet");
    }

    if (cacheBuffer.capacity() < logSize) {
      cacheBuffer = ByteBuffer.allocate(logSize);
    } else {
      cacheBuffer.clear();
    }

    putLogHeadersInteral(null, log, cacheBuffer,
        MutationLogEntrySerializationCompactFormat.fieldsInOrderForPersistedLogs);
    return cacheBuffer;
  }
}
