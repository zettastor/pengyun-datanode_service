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

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.datanode.segment.datalog.MutationLogEntry.LogStatus;

public abstract class MutationLogEntryReaderCompactImpl extends MutationLogEntryReader {
  protected static final Logger logger = LoggerFactory
      .getLogger(MutationLogEntryReaderCompactImpl.class);
  protected DataInputStream dataInputStream;
  protected long uuid = 0;
  protected long logId = LogImage.INVALID_LOG_ID;
  protected LogStatus status = null;
  // the offset in a segment where a change starts to apply to
  protected long offset = -1;
  protected int length = -1;
  protected long checksum = 0;
  protected long volumeId = 0;
  protected int segIndex = 0;
  protected long archiveId = 0;
  protected boolean eof = false;

  @Override
  public void open(InputStream inputStream) throws IOException {
    BufferedInputStream bis = new BufferedInputStream(inputStream);
    dataInputStream = new DataInputStream(bis);
    eof = false;
  }

  public void readInteral() throws IOException {
    int fieldsRead = 0;

    try {
      for (MutationLogEntrySerializationCompactFormat.FieldType fieldType
          : getFormat()) {
        switch (fieldType) {
          case UUID:

            uuid = dataInputStream.readLong();
            break;
          case LOG_ID:

            logId = dataInputStream.readLong();
            break;
          case LOG_STATUS:

            byte statusByte = dataInputStream.readByte();
            if (statusByte
                == MutationLogEntrySerializationCompactFormat.LOG_STATUS_COMMITED_FIELD_VALUE) {
              status = LogStatus.Committed;
            } else if (statusByte
                == MutationLogEntrySerializationCompactFormat
                .LOG_STATUS_ABORTED_CONFIRMED_FIELD_VALUE) {
              status = LogStatus.AbortedConfirmed;
            } else if (statusByte
                == MutationLogEntrySerializationCompactFormat.LOG_STATUS_ABORTED_FIELD_VALUE) {
              status = LogStatus.Aborted;
            } else if (statusByte
                == MutationLogEntrySerializationCompactFormat.LOG_STATUS_CREATED_FIELD_VALUE) {
              status = LogStatus.Created;
            } else {
              throw new RuntimeException("Inappropriate log status " + statusByte);
            }
            break;
          case LOG_OFFSET:

            offset = dataInputStream.readLong();
            break;
          case LOG_LENGTH:

            length = dataInputStream.readInt();
            break;
          case LOG_CHECKSUM:

            checksum = dataInputStream.readLong();
            break;
          case VOLUME_ID:

            volumeId = dataInputStream.readLong();
            break;
          case SEG_INDEX:

            segIndex = dataInputStream.readInt();
            break;
          default:
            throw new RuntimeException("not supported");
        }
        fieldsRead++;
      }
    } catch (EOFException e) {
      if (fieldsRead > 0) {
        logger.warn(
            "Read the end of the log file but still need to read more log fields. " 
                + "{} fields have been read",
            fieldsRead);
        throw new IOException();
      } else {
        logger.info("read the end of input stream");
        eof = true;
        return;
      }
    }
  }

  @Override
  public void close() throws IOException {
    if (dataInputStream != null) {
      dataInputStream.close();
    }
  }

  @Override
  public DataInputStream getInputStream() {
    return dataInputStream;
  }
}
