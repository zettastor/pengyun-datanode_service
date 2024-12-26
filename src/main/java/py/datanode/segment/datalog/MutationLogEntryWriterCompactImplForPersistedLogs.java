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
