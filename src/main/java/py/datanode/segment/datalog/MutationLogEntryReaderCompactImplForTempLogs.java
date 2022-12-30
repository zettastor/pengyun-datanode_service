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
