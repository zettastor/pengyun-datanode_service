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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.segment.SegId;

public class MutationLogEntryWriterSaveJsonImpl {
  private static final Logger logger = LoggerFactory
      .getLogger(MutationLogEntryWriterSaveJsonImpl.class);

  private ObjectMapper objectMapper;
  private OutputStream externalOutputStream;
  private byte[] newline;

  public MutationLogEntryWriterSaveJsonImpl() {
    try {
      objectMapper = new ObjectMapper();
      objectMapper.getFactory().disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET);
      newline = System.getProperty("line.separator").getBytes();
    } catch (Exception e) {
      logger.error("no way we can catch this.", e);
      throw new RuntimeException(e);
    }
  }

  public void close() throws IOException {
    if (objectMapper != null) {
      objectMapper = null;
    }

    if (externalOutputStream != null) {
      externalOutputStream.close();
    }
  }

  public void open(SegId segId, String filePath) throws IOException {
    String fileName = MutationLogEntrySaveHelper.buildSaveLogFileNameWithSegId(segId);
    Path pathToLogFile = FileSystems.getDefault().getPath(filePath, fileName);
    try {
      if (!Files.exists(pathToLogFile.getParent())) {
        Files.createDirectories(pathToLogFile.getParent());
      }

      Files.deleteIfExists(pathToLogFile);
      Files.createFile(pathToLogFile);
    } catch (FileAlreadyExistsException e) {
      logger.warn("file:{} should not be exist", pathToLogFile.toString(), e);
    }
    this.externalOutputStream = new FileOutputStream(pathToLogFile.toFile(), false);
  }

  public int write(List<MutationLogEntry> logs) throws IOException {
    int writeCount = 0;
    for (MutationLogEntry log : logs) {
      if (log.getLogId() == LogImage.INVALID_LOG_ID) {
        continue;
      }
      write(log);
      writeCount++;
    }
    return writeCount;
  }

  public int write(MutationLogEntry log) throws IOException {
    if (log == null) {
      logger.error("can not save null log");
      throw new IOException();
    }

    MutationLogEntryForSave saveLog = MutationLogEntrySaveHelper.buildFromLog(log);
    objectMapper.writeValue(this.externalOutputStream, saveLog);

    externalOutputStream.write(newline);
    return 1;
  }

}
