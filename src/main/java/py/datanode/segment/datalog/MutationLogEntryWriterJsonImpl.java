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

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import org.apache.commons.lang3.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.segment.SegId;
import py.datanode.segment.datalog.MutationLogEntry.LogStatus;

public class MutationLogEntryWriterJsonImpl extends MutationLogEntryWriter {
  private static final Logger logger = LoggerFactory
      .getLogger(MutationLogEntryWriterJsonImpl.class);
  private static byte[] emptyBytes;
  private static byte[] returnCharBytes;
  private JsonGenerator jsonGenerator;
  private ByteArrayOutputStream fixedSizeOutputStream;
  private OutputStream externalOutputStream;
  private int fixedSizeForLog;

  public MutationLogEntryWriterJsonImpl() {
    this.fixedSizeForLog = MutationLogEntrySerializationJsonFormat.SERIALIZED_LOG_SIZE;
    if (emptyBytes == null) {
      emptyBytes = new byte[1];
      emptyBytes[0] = MutationLogEntrySerializationJsonFormat.PADDING_BYTE;
    }

    if (returnCharBytes == null) {
      returnCharBytes = new byte[2];
      returnCharBytes[0] = MutationLogEntrySerializationJsonFormat.RETURN_CHAR_BYTE;
      returnCharBytes[1] = MutationLogEntrySerializationJsonFormat.CHANGE_LINE_CHAR_BYTE;
    }

    fixedSizeOutputStream = new ByteArrayOutputStream(fixedSizeForLog);
    JsonFactory jsonFactory = new JsonFactory();
    try {
      jsonGenerator = jsonFactory.createGenerator(fixedSizeOutputStream, JsonEncoding.UTF8);
    } catch (IOException e) {
      logger.error("no way we can catch this.", e);
      throw new RuntimeException(e);
    }

  }

  @Override
  public void open(OutputStream outputStream) throws IOException {
    externalOutputStream = outputStream;
  }

  @Override
  public int write(SegId segId, MutationLogEntry log) throws IOException {
    throw new NotImplementedException("this is a MutationLogEntryWriterJsonImpl");
  }
  
  @Override
  public int write(MutationLogEntry log) throws JsonGenerationException, IOException {
    if (log == null) {
      return -1;
    }

    jsonGenerator.writeStartObject();
    jsonGenerator.writeNumberField(MutationLogEntrySerializationJsonFormat.LOG_UUID_FIELD_NAME,
        log.getUuid());
    jsonGenerator.writeNumberField(MutationLogEntrySerializationJsonFormat.LOG_ID_FIELD_NAME,
        log.getLogId());
    if (log.getStatus() != LogStatus.Committed && log.getStatus() != LogStatus.AbortedConfirmed) {
      throw new JsonGenerationException(
          "wrong log status: " + log.getStatus() + " committed or aborted expected");
    }

    if (!log.isApplied()) {
      throw new JsonGenerationException("wrong log status: log is not applied yet");
    }

    jsonGenerator.writeStringField(MutationLogEntrySerializationJsonFormat.LOG_STATUS_FIELD_NAME,
        (log.getStatus() == LogStatus.Committed)
            ? MutationLogEntrySerializationJsonFormat.LOG_STATUS_COMMITED_FIELD_VALUE
            : MutationLogEntrySerializationJsonFormat.LOG_STATUS_ABORTED_CONFIRMED_FIELD_VALUE);
    jsonGenerator
        .writeNumberField(MutationLogEntrySerializationJsonFormat.LOG_DATA_OFFSET_FIELD_NAME,
            log.getOffset());
    jsonGenerator
        .writeNumberField(MutationLogEntrySerializationJsonFormat.LOG_DATA_LENGTH_FIELD_NAME,
            log.getLength());

    jsonGenerator.writeEndObject();
    jsonGenerator.flush();
    int usedSize = fixedSizeOutputStream.size();
   
   
    int paddingSize = fixedSizeForLog - usedSize;
    if (paddingSize < 0) {
      String errMsg = "the size " + usedSize + " of the serialized mutation log is larger than "
          + fixedSizeForLog
          + ". The mutationLogEntry is " + log;
      logger.error(errMsg);
      throw new JsonGenerationException(errMsg);
    }

    while (paddingSize > 2) {
      fixedSizeOutputStream.write(emptyBytes);
      paddingSize--;
    }

    if (paddingSize > 0) {
      fixedSizeOutputStream.write(returnCharBytes, 0, paddingSize);
    }

    fixedSizeOutputStream.flush();
    fixedSizeOutputStream.writeTo(externalOutputStream);
    fixedSizeOutputStream.reset();
    return -1;
  }

  @Override
  public void close() throws IOException {
    if (jsonGenerator != null) {
     
      jsonGenerator.close();
    }
   
    if (externalOutputStream != null) {
      externalOutputStream.close();
    }
  }

  @Override
  public OutputStream getOutputStream() {
    return externalOutputStream;
  }

  @Override
  protected ByteBuffer putLogHeaders(SegId segId, MutationLogEntry log) {
    throw new NotImplementedException("this is a MutationLogEntryWriterJsonImpl");
  }
}
