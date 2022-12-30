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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import org.apache.commons.lang3.NotImplementedException;
import py.archive.segment.SegId;
import py.common.struct.Pair;

public class MutationLogEntryReaderJsonImpl extends MutationLogEntryReader {
  private JsonParser jsonParser;
  private JsonFactory jsonFactory;
  private DataInputStream externalInputStream;

  public MutationLogEntryReaderJsonImpl() {
    jsonFactory = new JsonFactory();
  }

  @Override
  public void open(InputStream inputStream) throws IOException {
    jsonParser = jsonFactory.createJsonParser(inputStream);
    externalInputStream = new DataInputStream(inputStream);
  }

  @Override
  public MutationLogEntry read() throws IOException {
    JsonToken token = jsonParser.nextToken();

    if (token != JsonToken.START_OBJECT) {
      throw new IOException("Expected data to start with an Object");
    }

    long uuid = 0;
    long logId = LogImage.INVALID_LOG_ID;
    MutationLogEntry.LogStatus status = null;
    long offset = -1;

    int length = -1;
    int snapshotVersion = 0;

    while (jsonParser.nextToken() != JsonToken.END_OBJECT) {
      String fieldName = jsonParser.getCurrentName();

      jsonParser.nextToken();
      if (fieldName.equals(MutationLogEntrySerializationJsonFormat.LOG_UUID_FIELD_NAME)) {
        uuid = jsonParser.getLongValue();
      } else if (fieldName.equals(MutationLogEntrySerializationJsonFormat.LOG_ID_FIELD_NAME)) {
        logId = jsonParser.getLongValue();
      } else if (fieldName.equals(MutationLogEntrySerializationJsonFormat.LOG_STATUS_FIELD_NAME)) {
        status = MutationLogEntrySerializationJsonFormat.LOG_STATUS_COMMITED_FIELD_VALUE
            .equals(jsonParser
                .getText()) ? MutationLogEntry.LogStatus.Committed
            : MutationLogEntry.LogStatus.AbortedConfirmed;
      } else if (fieldName
          .equals(MutationLogEntrySerializationJsonFormat.LOG_DATA_OFFSET_FIELD_NAME)) {
        offset = jsonParser.getLongValue();
      } else if (fieldName
          .equals(MutationLogEntrySerializationJsonFormat.LOG_DATA_LENGTH_FIELD_NAME)) {
        length = jsonParser.getIntValue();
      } else if (fieldName
          .equals(MutationLogEntrySerializationJsonFormat.LOG_SNAPSHOT_VERSION_FIELD_NAME)) {
        snapshotVersion = jsonParser.getIntValue();
      } else {
        throw new IOException("Unrecognized field '" + fieldName + "'");
      }
    }

    MutationLogEntry logEntry = MutationLogEntryFactory
        .createLogFromPersistentStore(uuid, logId, offset, length,
            status, snapshotVersion);

    return logEntry;
  }

  @Override
  public void close() throws IOException {
    if (jsonParser != null) {
      jsonParser.close();
    }
    if (externalInputStream != null) {
      externalInputStream.close();
    }
  }

  @Override
  protected MutationLogEntrySerializationCompactFormat.FieldType[] getFormat() {
    return null;
  }

  @Override
  public DataInputStream getInputStream() {
    return externalInputStream;
  }

  @Override
  public Pair<SegId, MutationLogEntry> readLogAndSegment() throws IOException {
    throw new NotImplementedException("");
  }
}
