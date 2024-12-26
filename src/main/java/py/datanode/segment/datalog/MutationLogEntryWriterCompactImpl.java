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
import org.apache.commons.lang.Validate;
import py.archive.segment.SegId;
import py.datanode.segment.datalog.MutationLogEntry.LogStatus;
import py.datanode.segment.datalog.MutationLogEntrySerializationCompactFormat.FieldType;

public abstract class MutationLogEntryWriterCompactImpl extends MutationLogEntryWriter {
  private OutputStream externalOutputStream;

  public void open(OutputStream outputStream) throws IOException {
    externalOutputStream = outputStream;
  }

  protected int writeLogHeader(SegId segId, MutationLogEntry log) throws IOException {
    if (log == null) {
      return 0;
    }

    ByteBuffer byteBuffer = putLogHeaders(segId, log);

    int size = byteBuffer.position();

    externalOutputStream.write(byteBuffer.array(), byteBuffer.arrayOffset(), size);

    return size;
  }

  /**
   * close all resources.
   */
  @Override
  public void close() throws IOException {
    if (externalOutputStream != null) {
      externalOutputStream.close();
    }
  }

  protected void putLogHeadersInteral(SegId segId, MutationLogEntry log, ByteBuffer buffer,
      FieldType[] fields) {
    for (MutationLogEntrySerializationCompactFormat.FieldType fieldType : fields) {
      switch (fieldType) {
        case LOG_ID:
          buffer.putLong(log.getLogId());
          break;
        case UUID:
          buffer.putLong(log.getUuid());
          break;
        case LOG_STATUS:
          LogStatus status = log.getStatus();

          if (status == LogStatus.Committed) {
            buffer.put(MutationLogEntrySerializationCompactFormat.LOG_STATUS_COMMITED_FIELD_VALUE);
          } else if (status == LogStatus.AbortedConfirmed) {
            buffer.put(MutationLogEntrySerializationCompactFormat
                .LOG_STATUS_ABORTED_CONFIRMED_FIELD_VALUE);
          } else if (status == LogStatus.Created) {
            buffer.put(MutationLogEntrySerializationCompactFormat.LOG_STATUS_CREATED_FIELD_VALUE);
          } else if (status == LogStatus.Aborted) {
            buffer.put(MutationLogEntrySerializationCompactFormat.LOG_STATUS_ABORTED_FIELD_VALUE);
          } else {
            throw new RuntimeException(
                "wrong log status: " + status + " committed or aborted expected");
          }
          break;
        case LOG_OFFSET:

          buffer.putLong(log.getOffset());
          break;
        case LOG_LENGTH:

          buffer.putInt(log.getLength());
          break;
        case LOG_CHECKSUM:

          buffer.putLong(log.getChecksum());
          break;
        case VOLUME_ID:

          Validate.notNull(segId);
          buffer.putLong(segId.getVolumeId().getId());
          break;
        case SEG_INDEX:

          Validate.notNull(segId);
          buffer.putInt(segId.getIndex());
          break;
        default:
          throw new RuntimeException("field type " + fieldType + " is unknown");
      }
    }
  }

  @Override
  public OutputStream getOutputStream() {
    return externalOutputStream;
  }
}
