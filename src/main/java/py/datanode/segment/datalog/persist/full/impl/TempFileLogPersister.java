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

package py.datanode.segment.datalog.persist.full.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.DirectoryStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.segment.SegId;
import py.common.Utils;
import py.common.struct.Pair;
import py.datanode.segment.datalog.LogImage;
import py.datanode.segment.datalog.MutationLogEntry;
import py.datanode.segment.datalog.MutationLogEntryReader;
import py.datanode.segment.datalog.MutationLogEntryWriter;
import py.datanode.segment.datalog.MutationLogManager;
import py.datanode.segment.datalog.MutationLogReaderWriterFactoryForTempStore;
import py.datanode.segment.datalog.SegmentLogMetadata;
import py.datanode.segment.datalog.persist.full.TempLogPersister;

/**
 * Persist logs to a directory.
 */
public class TempFileLogPersister implements TempLogPersister {
  private static final Logger logger = LoggerFactory.getLogger(TempFileLogPersister.class);
  private static long MAX_FILE_FOR_STORING_LOGS = 1024L * 1024L * 64;
  private static String delimitor = "abcdef";
  private Path rootDirPath;
  private String rootDirString;
  private HashMap<SegId, Long> idsOfLastPersistedLogs;
  private List<Path> logFiles;

  private long currentLogFileSize;
  private Path currentLogFile;
  private int fileId;
  private MutationLogEntryWriter currentWriter;
  private MutationLogReaderWriterFactoryForTempStore factory;
  private Object lock;
  private volatile long maxSizeLogFileInBytes = MAX_FILE_FOR_STORING_LOGS;

  public TempFileLogPersister(String dirName) throws IOException {
    try {
      rootDirPath = FileSystems.getDefault().getPath(dirName);
      rootDirString = dirName;
      if (!Files.exists(rootDirPath)) {
        Files.createDirectory(rootDirPath);
      } else {
        logger.info("Path for persisting logs is {}", rootDirPath);
      }
    } catch (Exception e) {
      logger.warn("{} has a wrong error format", dirName, e);
      throw new IOException(e);
    }

    lock = new Object();
    factory = new MutationLogReaderWriterFactoryForTempStore();

    logFiles = getAllLogFiles(rootDirPath);
    if (logFiles.isEmpty()) {
      currentLogFile = null;
      currentLogFileSize = 0;
      currentWriter = null;
      fileId = 1;
    } else {
      currentLogFile = logFiles.get(logFiles.size() - 1);
      MutationLogEntryWriter writer = factory.generateWriter();
      try {
        currentLogFileSize = Files.size(currentLogFile);
        writer.open(new FileOutputStream(currentLogFile.toFile(), true));
        currentWriter = writer;
      } catch (Exception e) {
        logger.error("can't open {} file to persist log", currentLogFile.getFileName().toString());
        throw new IOException(e);
      }
    }
    idsOfLastPersistedLogs = new HashMap<>();
  }

  public static String serializeMap(HashMap<SegId, Long> map) throws IOException {
    if (map == null || map.isEmpty()) {
      return "";
    }

    ObjectMapper mapper = new ObjectMapper();

    StringBuilder builder = new StringBuilder();
    for (Map.Entry<SegId, Long> entry : map.entrySet()) {
      builder.append(mapper.writeValueAsString(entry.getKey()));
      builder.append(delimitor);
      builder.append(mapper.writeValueAsString(entry.getValue()));
      builder.append("\n");
    }

    return builder.toString();
  }

  public static HashMap<SegId, Long> deserializeMap(File file) throws Exception {
    HashMap<SegId, Long> map = new HashMap<>();
    ObjectMapper mapper = new ObjectMapper();
    String strLine;
    BufferedReader br = null;
    try {
      FileInputStream fstream = new FileInputStream(file);
      br = new BufferedReader(new InputStreamReader(fstream));

      while ((strLine = br.readLine()) != null) {
        String[] string = strLine.split(delimitor);
        Validate.isTrue(string.length == 2);
        SegId segId = mapper.readValue(string[0], SegId.class);
        Long logId = mapper.readValue(string[1], Long.class);
        map.put(segId, logId);
      }
    } finally {
      if (br != null) {
        br.close();
      }
    }
    return map;
  }

  @Override
  public void persistLogs(SegId segId, List<MutationLogEntry> logs) throws IOException {
    if (rootDirPath == null) {
      return;
    }
    boolean createNewFile = false;
    synchronized (lock) {
      if (currentLogFileSize > maxSizeLogFileInBytes) {
        closeCurrentLogFile();
        createNewFile = true;
      }

      if (currentLogFile == null || createNewFile) {
        String newFileName = getNewFileName();
        logger.debug("new file name is {}", newFileName);
        MutationLogEntryWriter writer = factory.generateWriter();
        try {
          Path newFilePath = generateLogFileObject(newFileName);
          writer.open(new FileOutputStream(newFilePath.toFile(), false));

          currentLogFile = newFilePath;
          currentWriter = writer;
          currentLogFileSize = 0;
          logFiles.add(currentLogFile);
        } catch (Exception e) {
          logger.error("can't open {} file to persist log", newFileName);
          throw new IOException(e);
        }
      }

      for (MutationLogEntry log : logs) {
        int logSize = currentWriter.write(segId, log);
        logger.debug("log size is {} and log is {}", logSize, log);
        currentLogFileSize += logSize;
        idsOfLastPersistedLogs.put(segId, log.getLogId());
      }
    }
  }

  private void closeCurrentLogFile() throws IOException {
    if (currentWriter == null) {
      return;
    }

    try {
      currentWriter.close();
    } catch (IOException e) {
      logger.error("can't close the current log file for write {}", currentLogFile, e);
      throw e;
    }

    if (idsOfLastPersistedLogs.isEmpty()) {
      return;
    }

    Path mapFilePath = generateLogFileObject(getLogsMapperFileName(currentLogFile));
    String serialization = serializeMap(idsOfLastPersistedLogs);
    FileUtils.writeStringToFile(mapFilePath.toFile(), serialization);
  }

  private String getNewFileName() {
    String newFileName = fileId + "";
    fileId++;
    return newFileName;
  }

  private String getLogsMapperFileName(Path logFilePath) {
    return logFilePath.getFileName() + ".map";
  }

  private Path getLogsMapperFilePath(Path logFilePath) {
    return FileSystems.getDefault().getPath(rootDirString, getLogsMapperFileName(logFilePath));
  }

  @Override

  public void insertMissingLogs(MutationLogManager mutationLogManager, boolean cleanLogs) {
    Map<SegId, Long> largestLogAtSegments = new HashMap<>();
    try {
      List<SegmentLogMetadata> logMetadatas = mutationLogManager.getSegments(null);
      for (SegmentLogMetadata logMetadata : logMetadatas) {
        LogImage image = logMetadata.getLogImage(0);
        largestLogAtSegments.put(logMetadata.getSegId(), logMetadata.getLogImage(0).getPlId());
        logger.warn("now dump segId: {} and logImage {}", logMetadata.getSegId(), image);
      }

      MutationLogEntryReader reader = factory.generateReader();
      for (Path logFile : logFiles) {
        boolean fileOpened = false;
        boolean readFile = false;
        try {
          Path logMapperFile = getLogsMapperFilePath(logFile);
          logger.debug("read {} mapper file", logMapperFile);

          HashMap<SegId, Long> mapFromSegIdToLogId = deserializeMap(logMapperFile.toFile());

          for (Map.Entry<SegId, Long> entry : mapFromSegIdToLogId.entrySet()) {
            Long plId = largestLogAtSegments.get(entry.getKey());
            logger.debug("plId {} and entry read from file mapper {}", plId, entry);
            if (plId != null && plId < entry.getValue()) {
              readFile = true;
            }
          }
        } catch (Exception e) {
          logger.info("caught an exception", e);
          readFile = true;
        }

        if (!readFile) {
          logger.debug("no need to read {}", logFile.getFileName());
        } else {
          logger.warn("need to read {}", logFile.getFileName());
          try {
            reader.open(new FileInputStream(logFile.toFile()));
            fileOpened = true;

            while (true) {
              Pair<SegId, MutationLogEntry> logAtSegment = null;
              try {
                logAtSegment = reader.readLogAndSegment();
              } catch (IOException e) {
                logger
                    .warn("caught an exception when reading logfile {}", logFile.getFileName(), e);
              }

              if (logAtSegment == null) {
                break;
              }

              SegmentLogMetadata segLogMetadata = mutationLogManager
                  .getSegment(logAtSegment.getFirst());
              if (segLogMetadata == null) {
                logger.warn("segment {} doesn't exist at the system", logAtSegment.getFirst());
              } else {
                try {
                  MutationLogEntry entry = segLogMetadata.insertLog(logAtSegment.getSecond());
                  logger.debug("insert log {} and after insert log is {}", logAtSegment.getSecond(),
                      entry);
                } catch (Exception e) {
                  logger.info("log {} might exist in the system and too small",
                      logAtSegment.getSecond());
                }
              }
            }
          } catch (Exception e) {
            logger.warn("caught an exception", e);
          } finally {
            if (fileOpened) {
              reader.close();
            }
          }
        }
      }
    } catch (Exception e) {
      logger.warn("caught an exception", e);
    } finally {
      if (cleanLogs && rootDirPath != null) {
        try {
          FileUtils.cleanDirectory(rootDirPath.toFile());
          logFiles.clear();
        } catch (IOException e) {
          logger.warn("can't remove files under {}", rootDirPath);
        }
      }
    }
  }

  public List<Path> getAllFiles(Path rootDir) throws IOException {
    List<Path> files = new ArrayList<Path>();
    DirectoryStream<Path> filesUnderRootDir = null;
    try {
      filesUnderRootDir = Files.newDirectoryStream(rootDir);
      for (Path file : filesUnderRootDir) {
        files.add(file);
      }
    } catch (Exception e) {
      logger.error(
          "get all log file from segment failed, Current process open file handler count: [{}]",
          Utils.getOpenFileAmountByPid(), e);
    } finally {
      try {
        if (filesUnderRootDir != null) {
          filesUnderRootDir.close();
        }
      } catch (IOException ioe) {
        logger.error("failed to close a directory stream", ioe);
      }
    }

    Collections.sort(files, new Comparator<Path>() {
      public int compare(Path o1, Path o2) {
        return o1.toFile().compareTo(o2.toFile());
      }
    });
    return files;
  }

  public List<Path> getAllLogFiles(Path rootDir) throws IOException {
    List<Path> files = getAllFiles(rootDir);
    List<Path> logFiles = new ArrayList<>();
    if (files != null) {
      for (Path file : files) {
        if (file.toString().indexOf("map") < 0) {
          logFiles.add(file);
        }
      }
    }
    return logFiles;
  }

  public List<Path> getAllLogMapFiles(Path rootDir) throws IOException {
    List<Path> files = getAllFiles(rootDir);
    List<Path> logMapFiles = new ArrayList<>();
    if (files != null) {
      for (Path file : files) {
        if (file.toString().indexOf("map") >= 0) {
          logMapFiles.add(file);
        }
      }
    }
    return logMapFiles;
  }

  private Path generateLogFileObject(String fileName) throws IOException {
    Path pathToLogFile = FileSystems.getDefault().getPath(rootDirString, fileName);
    try {
      Files.createFile(pathToLogFile);
    } catch (FileAlreadyExistsException e) {
      logger.warn("Log file {} exists. Do nothing", pathToLogFile);
    }

    return pathToLogFile;
  }

  public long getMaxSizeLogFileInBytes() {
    return maxSizeLogFileInBytes;
  }

  public void setMaxSizeLogFileInBytes(long maxSizeLogFileInBytes) {
    this.maxSizeLogFileInBytes = maxSizeLogFileInBytes;
  }

  @Override
  public void close() throws IOException {
    closeCurrentLogFile();
  }
}
