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

package py.datanode.segment.datalog.persist;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.segment.SegId;
import py.datanode.segment.datalog.MutationLogEntryReader;
import py.datanode.segment.datalog.MutationLogEntryWriter;
import py.datanode.segment.datalog.MutationLogReaderWriterFactory;

public class LogStorageMetadata {
  private static final Logger logger = LoggerFactory.getLogger(LogStorageMetadata.class);

  private final int capacity;

  private final MutationLogReaderWriterFactory readerWriterFactory;

  private int numLogs;
  private long maxLogId;
  private MutationLogEntryWriter logWriter;
  private MutationLogEntryReader logReader;
  private Path logFile;
  private List<Path> logFiles;
  private SegId segId;

  public LogStorageMetadata(MutationLogReaderWriterFactory readerWriterFactory, int capacity,
      int numLogs,
      long maxLogId, SegId segId) {
    this.capacity = capacity;
    this.numLogs = numLogs;
    this.maxLogId = maxLogId;
    this.segId = segId;
    this.readerWriterFactory = readerWriterFactory;
    this.logFiles = Collections.synchronizedList(new ArrayList<Path>());
  }

  public SegId getSegId() {
    return segId;
  }

  public boolean moreSpace() {
    return numLogs < capacity;
  }

  public int leftSpace() {
    return capacity - numLogs;
  }

  public int getNumLogs() {
    return numLogs;
  }

  public long getMaxLogId() {
    return maxLogId;
  }

  public void setMaxLogId(long maxLogId) {
    this.maxLogId = maxLogId;
  }

  public int getCapacity() {
    return capacity;
  }

  public MutationLogEntryWriter getLogWriter() {
    return logWriter;
  }

  public void setLogWriter(MutationLogEntryWriter logWriter) {
    this.logWriter = logWriter;
  }

  public MutationLogEntryReader getLogReader() {
    if (logReader == null) {
      FileInputStream fileInputStream = null;
      try {
        fileInputStream = new FileInputStream(logFile.toFile());
        logReader = readerWriterFactory.generateReader();
        logReader.open(fileInputStream);
      } catch (Exception e) {
        try {
          if (logReader != null) {
            logReader.close();
            logReader = null;
          }
        } catch (IOException ioe) {
          logger.error("Can't open {} to read", logFile);
        }
      }
    }
    return logReader;
  }

  public void setLogReader(MutationLogEntryReader logReader) {
    this.logReader = logReader;
  }

  public void incNumLogs() {
    this.numLogs++;
  }

  public void addNumLogs(int logCount) {
    this.numLogs += logCount;
  }

  public Path getLogFile() {
    return logFile;
  }

  public void setLogFile(Path logFile) {
    this.logFile = logFile;
  }

  public void close() {
    try {
      if (logWriter != null) {
        logWriter.close();
        logWriter = null;
      }
      if (logReader != null) {
        logReader.close();
        logReader = null;
      }
    } catch (IOException e) {
      logger.error("close resources failed", e);
    }
    this.numLogs = 0;
  }

  public void removeLogFileAtHead() {
    Path filePath = null;
    Validate.isTrue(logFiles.size() > 1);
    filePath = logFiles.remove(0);

    try {
      Files.deleteIfExists(filePath);
      logger.info("delete log file {} for {}", filePath.getFileName().toString(), segId);
    } catch (IOException e) {
      logger.error("try to delete file:{} failed", filePath.toString(), e);
    }
  }

  public void addLogFilesToTail(List<Path> logFiles) {
    if (logFiles != null && logFiles.size() != 0) {
      this.logFiles.addAll(logFiles);
    }
  }

  public void addLogFileToTail(Path logFile) {
    if (!logFiles.contains(logFile)) {
      logFiles.add(logFile);
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("[LogStorageMetadata: numLogs=").append(this.numLogs).append(",maxLogId=")
        .append(this.maxLogId)
        .append(",capacity=").append(this.capacity).append(",logFile=")
        .append(this.logFile == null ? "null" : this.logFile.toString()).append(", logFiles=")
        .append(this.logFiles).append("]");
    return sb.toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof LogStorageMetadata)) {
      return false;
    }

    LogStorageMetadata other = (LogStorageMetadata) obj;
    return this.segId.equals(other.segId);
  }

  @Override
  public int hashCode() {
    return this.segId.hashCode();
  }

  public int getLogFileCount() {
    return this.logFiles.size();
  }

  public static class CounterComparator implements Comparator<LogStorageMetadata> {
    @Override
    public int compare(LogStorageMetadata o1, LogStorageMetadata o2) {
      return (o1.logFiles.size() - o2.logFiles.size() > 0) ? -1 : 1;
    }
  }
}
