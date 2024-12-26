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
