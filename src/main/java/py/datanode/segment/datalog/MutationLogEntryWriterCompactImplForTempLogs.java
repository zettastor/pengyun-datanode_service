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
import java.io.OutputStream;
import java.nio.ByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.segment.SegId;

public class MutationLogEntryWriterCompactImplForTempLogs extends
    MutationLogEntryWriterCompactImpl {
  private static final Logger logger = LoggerFactory
      .getLogger(MutationLogEntryWriterCompactImplForTempLogs.class);
  private ByteBuffer cacheBuffer;
  private int logSize;

  public MutationLogEntryWriterCompactImplForTempLogs() {
    logSize = MutationLogEntrySerializationCompactFormat.SERIALIZED_LOG_SIZE_FOR_TEMP_LOGS;
    cacheBuffer = ByteBuffer.allocate(logSize);
  }

  public MutationLogEntryWriterCompactImplForTempLogs(ByteBuffer cacheBuffer) {
    logSize = MutationLogEntrySerializationCompactFormat.SERIALIZED_LOG_SIZE_FOR_TEMP_LOGS;
    this.cacheBuffer = cacheBuffer;
  }

  @Override
  protected ByteBuffer putLogHeaders(SegId segId, MutationLogEntry log) {
    if (cacheBuffer.capacity() < logSize) {
      cacheBuffer = ByteBuffer.allocate(logSize);
    } else {
      cacheBuffer.clear();
    }

    putLogHeadersInteral(segId, log, cacheBuffer,
        MutationLogEntrySerializationCompactFormat.fieldsInOrderForTempLogs);

    return cacheBuffer;
  }

  @Override
  public int write(MutationLogEntry log) throws IOException {
    throw new RuntimeException("not implemented method");
  }

  @Override
  public int write(SegId segId, MutationLogEntry log) throws IOException {
    int headSize = writeLogHeader(segId, log);

    byte[] logData = log.getData();
    if (logData != null && logData.length > 0) {
      OutputStream os = getOutputStream();
      os.write(logData);
      os.flush();
      headSize += logData.length;
    }

    logger.debug("segId {} write log {}", segId, log);

    return headSize;
  }
}
