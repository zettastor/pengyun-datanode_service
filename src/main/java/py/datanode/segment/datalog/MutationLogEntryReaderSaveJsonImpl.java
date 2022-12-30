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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.segment.SegId;
import py.exception.NoAvailableBufferException;

public class MutationLogEntryReaderSaveJsonImpl {
  private static final Logger logger = LoggerFactory
      .getLogger(MutationLogEntryReaderSaveJsonImpl.class);
  private InputStream externalInputStream;
  private ObjectMapper objectMapper;
  private String fileName;

  private Long archiveId;

  public MutationLogEntryReaderSaveJsonImpl() {
    try {
      objectMapper = new ObjectMapper();
      objectMapper.getFactory().disable(JsonParser.Feature.AUTO_CLOSE_SOURCE);
      archiveId = 0L;
    } catch (Exception e) {
      logger.error("no way we can catch this.", e);
      throw new RuntimeException(e);
    }
  }

  public void close() throws IOException {
    if (objectMapper != null) {
      objectMapper = null;
    }
    if (externalInputStream != null) {
      externalInputStream.close();
    }
  }

  public boolean open(SegId segId, String filePath) throws IOException {
    this.fileName = MutationLogEntrySaveHelper.buildSaveLogFileNameWithSegId(segId);
    Path pathToLogFile = FileSystems.getDefault().getPath(filePath, fileName);
    if (!Files.exists(pathToLogFile, LinkOption.NOFOLLOW_LINKS)) {
     
      return false;
    }
    this.externalInputStream = new FileInputStream(pathToLogFile.toFile());
    return true;
  }

  public List<MutationLogEntry> readAllLogFromFile() throws IOException {
    List<MutationLogEntry> logs = new ArrayList<MutationLogEntry>();

    BufferedReader br = new BufferedReader(new InputStreamReader(this.externalInputStream));
    String line;
    while ((line = br.readLine()) != null) {
      MutationLogEntry log = parseFrom(line);
      if (log == null) {
        break;
      }
      logs.add(log);
    }

    return logs;
  }

  private MutationLogEntry parseFrom(String jsonStr) {
    MutationLogEntry log = null;
    MutationLogEntryForSave saveLog = null;
    try {
      saveLog = objectMapper.readValue(jsonStr, MutationLogEntryForSave.class);
    } catch (Exception e) {
      logger.warn("fail to read save log from:{}", this.fileName);
    }
    if (saveLog == null) {
     
      return log;
    }
    try {
      saveLog.setArchiveId(archiveId);
      log = MutationLogEntrySaveHelper.buildFromSaveLog(saveLog);
     
    } catch (NoAvailableBufferException e) {
      logger.error("can not get available space for log:{} at file:{}", saveLog, this.fileName, e);
      throw new RuntimeException();
    }
    return log;
  }

  public Long getArchiveId() {
    return archiveId;
  }

  public void setArchiveId(Long archiveId) {
    this.archiveId = archiveId;
  }
}
