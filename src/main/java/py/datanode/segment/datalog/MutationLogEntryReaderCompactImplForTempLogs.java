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
import org.apache.commons.lang.Validate;
import org.apache.commons.lang3.NotImplementedException;
import py.archive.segment.SegId;
import py.common.struct.Pair;
import py.datanode.segment.datalog.MutationLogEntrySerializationCompactFormat.FieldType;
import py.exception.NoAvailableBufferException;

public class MutationLogEntryReaderCompactImplForTempLogs extends
    MutationLogEntryReaderCompactImpl {
  @Override
  protected FieldType[] getFormat() {
    return MutationLogEntrySerializationCompactFormat.fieldsInOrderForTempLogs;
  }

  @Override
  public MutationLogEntry read() throws IOException {
    throw new NotImplementedException("");
  }

  @Override
  public Pair<SegId, MutationLogEntry> readLogAndSegment() throws IOException {
    super.readInteral();
    if (eof) {
      return null;
    }

    MutationLogEntry log = null;

    if (length > 0) {
      byte[] data = new byte[length];
      int dataLenRead = dataInputStream.read(data);
      if (dataLenRead < data.length) {
        throw new IOException(
            data.length + " bytes are supposed to be read while only " + dataLenRead + " are read");
      }

      try {
        log = MutationLogEntryFactory
            .createLogForPrimary(uuid, logId, offset, data, checksum);
        logger.debug("segId  {} read a log from file {}", new SegId(volumeId, segIndex), log);
      } catch (NoAvailableBufferException e) {
        throw new IOException("no available buf");
      }

    } else {
      log = MutationLogEntryFactory
          .createEmptyLog(uuid, logId, offset, checksum, length);
      logger.debug("create an empty log {}", log);
    }
    log.setStatus(status);
    Validate.isTrue(volumeId != 0L);
    return new Pair<>(new SegId(volumeId, segIndex), log);
  }
}
