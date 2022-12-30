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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.DirectoryIteratorException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.segment.SegId;
import py.common.Utils;
import py.common.struct.Pair;
import py.datanode.configuration.LogPersistingConfiguration;
import py.datanode.exception.BadLogFileException;
import py.datanode.exception.LogIdNotFoundException;
import py.datanode.segment.datalog.LogImage;
import py.datanode.segment.datalog.MutationLogEntry;
import py.datanode.segment.datalog.MutationLogEntryReader;
import py.datanode.segment.datalog.MutationLogEntryWriter;
import py.datanode.segment.datalog.MutationLogReaderWriterFactory;
import py.exception.InvalidFormatException;

public class LogFileSystem implements LogStorageSystem {
  private static final Logger logger = LoggerFactory.getLogger(LogFileSystem.class);
  private static final AtomicLong REMOVE_FILE_COUNTER = new AtomicLong(0);
  private final int serializedLogSize;
  private final MutationLogReaderWriterFactory factory;
  private LogPersistingConfiguration cfg;

  private Map<SegId, LogStorageMetadata> logStorageMetadatas =
      new ConcurrentHashMap<SegId, LogStorageMetadata>();
  private long logFileCount = 0;
  private PriorityBlockingQueue<LogStorageMetadata> logStorageFileCountQueue = 
      new PriorityBlockingQueue<LogStorageMetadata>(
      1024,
          new LogStorageMetadata.CounterComparator());

  public LogFileSystem(LogPersistingConfiguration cfg, MutationLogReaderWriterFactory factory)
      throws IOException {
    this.cfg = cfg;
    logger.warn("configuration: {} for log file system", cfg);
    this.factory = factory;
    this.serializedLogSize = factory.getSerializedLogSize();
    init();
  }

  private static String getSegmentDirName(SegId segId) {
    return "seg_" + segId.getVolumeId().getId() + "." + segId.getIndex();
  }

  private static String getLogFileName(SegId segId, long id) {
    return getSegmentDirName(segId) + "_" + String.format("%020d", id);
  }

  private static long parseLogFileName(String fileName) throws InvalidFormatException {
    String[] strs = fileName.split("_");
    if (strs.length != 3) {
      throw new InvalidFormatException(fileName + " has a wrong format of log file");
    }

    try {
      return Long.valueOf(strs[2]);
    } catch (NumberFormatException e) {
      throw new InvalidFormatException(fileName + " has a wrong format", e);
    }
  }

  @Override
  public LogStorageMetadata getLogStorageToWrite(SegId segId) {
    return this.logStorageMetadatas.get(segId);
  }

  @Override
  public LogStorageMetadata createLogStorageToWrite(
      SegId segId, long id, LogStorageMetadata originalStorageMetadata) throws IOException {
    return createLogStorage(segId, id, originalStorageMetadata);
  }

  @Override
  public LogStorageMetadata getLatestLogStorageToRead(SegId segId) throws IOException {
    List<Path> allLogFiles = getAllLogFilesForSegment(segId);
    if (allLogFiles.isEmpty()) {
      return null;
    } else {
      Path latestLogFile = allLogFiles.get(allLogFiles.size() - 1);
      LogStorageMetadata storageMetadata = getLogFileMetadataWithErrorTolerentable(latestLogFile,
          segId);
      if (storageMetadata != null) {
        MutationLogEntryReader reader = factory.generateReader();
        FileInputStream fileInputStream = new FileInputStream(latestLogFile.toFile());
        reader.open(fileInputStream);
        storageMetadata.setLogReader(reader);
      }
      return storageMetadata;
    }
  }

  @Override
  public LogStorageMetadata getLogStorageToRead(
      SegId segId, long id) throws IOException, LogIdNotFoundException {
    try {
      List<Path> allLogFiles = getAllLogFilesForSegment(segId);
      if (allLogFiles.isEmpty()) {
        logger.warn("logs at {} is empty", segId);
        throw new LogIdNotFoundException("log file system has not directory", null, null);
      }

      Path fileMightContainId = null;
      Path nextToFileContainId = null;
      int lastIndex = allLogFiles.size() - 1;
      long minIdInFile = LogImage.INVALID_LOG_ID;
      for (; lastIndex >= 0; lastIndex--) {
        Path logFile = allLogFiles.get(lastIndex);
        minIdInFile = parseLogFileName(logFile.getFileName().toString());
        if (minIdInFile <= id) {
          fileMightContainId = logFile;
          break;
        } else {
          nextToFileContainId = logFile;
        }
      }

      LogStorageMetadata storageMetadata = null;
      if (fileMightContainId != null) {
        storageMetadata = getLogFileMetadataWithErrorTolerentable(fileMightContainId, segId);
        if (!(storageMetadata == null || id > storageMetadata.getMaxLogId())) {
          MutationLogEntryReader reader = factory.generateReader();
          FileInputStream fileInputStream = new FileInputStream(fileMightContainId.toFile());
          reader.open(fileInputStream);
          storageMetadata.setLogReader(reader);
          return storageMetadata;
        }
      }

      LogStorageMetadata nextStorageMetadata = null;
      if (nextToFileContainId != null) {
        nextStorageMetadata = getLogFileMetadataWithNoErrorTolerentable(nextToFileContainId, segId);
        nextStorageMetadata.setLogFile(nextToFileContainId);
      }

      throw new LogIdNotFoundException("Can't find " + id + " in the storage log system",
          storageMetadata,
          nextStorageMetadata);
    } catch (InvalidFormatException | BadLogFileException e) {
      logger.error("caught an exception, rethrow it as IOException", e);
      throw new IOException(e);
    }
  }

  @Override
  public LogStorageMetadata getOrCreateLogStorageToWrite(SegId segId, long id)
      throws IOException {
    LogStorageMetadata logStorageMetadata = getLogStorageToWrite(segId);
    if (logStorageMetadata == null) {
      logStorageMetadata = createLogStorageWhenNoLogFileExist(segId, id);
    }
    return logStorageMetadata;
  }

  @Override
  public Map<SegId, LogStorageMetadata> init() throws IOException {
    Path rootPath = FileSystems.getDefault().getPath(cfg.getRootDirName());

    Files.createDirectories(rootPath);

    DirectoryStream<Path> filesUnderRootDir = null;
    try {
      filesUnderRootDir = Files.newDirectoryStream(rootPath);
      for (Path segDir : filesUnderRootDir) {
        SegId segId = null;
        try {
          segId = parseSegmentIdFromDir(segDir.getFileName().toString());
        } catch (Exception e) {
          logger.error(
              "caught an exception when parsing seg dir name. Delete the current dir" + segDir, e);
          FileUtils.deleteDirectory(segDir.toFile());
          continue;
        }

        if (!initLogSystemForSeg(segId)) {
          logger.warn("init log meta data for segId {} failed", segId);
        }
      }
    } catch (DirectoryIteratorException x) {
      logger.error("caught an exception", x);
      throw x;
    } finally {
      if (filesUnderRootDir != null) {
        filesUnderRootDir.close();
      }
    }
    return logStorageMetadatas;
  }

  @Override
  public void removeLogStorage(SegId segId) {
    try {
      Path segDir = getSegmentDirPath(segId);
      if (Files.exists(segDir)) {
        logger.warn("remove the file directory: {}", segDir);
        FileUtils.deleteDirectory(segDir.toFile());
      }
    } catch (IOException e) {
      logger.warn("catch an exception when remove log {}", segId, e);
    }

    synchronized (this) {
      LogStorageMetadata logStorageMetadata = this.logStorageMetadatas.remove(segId);
      if (logStorageMetadata != null) {
        logStorageMetadata.close();
        this.logStorageFileCountQueue.remove(logStorageMetadata);
        this.logFileCount -= logStorageMetadata.getLogFileCount();
      }
    }
  }

  public List<Path> getAllLogFilesForSegment(SegId segId) throws IOException {
    return getAllLogFilesForSegment(getSegmentDirPath(segId));
  }

  private List<Path> getAllLogFilesForSegment(Path segDir) throws IOException {
    List<Path> files = new ArrayList<Path>();
    if (Files.exists(segDir)) {
      DirectoryStream<Path> filesUnderSegDir = null;
      try {
        filesUnderSegDir = Files.newDirectoryStream(segDir);
        for (Path file : filesUnderSegDir) {
          files.add(file);
        }
      } catch (Exception e) {
        logger.error(
            "get all log file from segment failed, Current process open file handler count: [{}]",
            Utils.getOpenFileAmountByPid(), e);
      } finally {
        try {
          if (filesUnderSegDir != null) {
            filesUnderSegDir.close();
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
    }
    return files;
  }

  private Pair<LogStorageMetadata, Path> initSegment(Path segDir, SegId segId)
      throws IOException, BadLogFileException {
    if (!Files.exists(segDir)) {
      return null;
    }

    List<Path> files = null;
    try {
      files = getAllLogFilesForSegment(segDir);
    } catch (IOException e) {
      logger.error("caught an io exception", e);
      throw e;
    }

    if (files.size() == 0) {
      logger.warn(
          "no file exists in the directory. Throw an IOException and expect outer can delete it");
      return null;
    } else {
      int fileSizeSupposedToBe = cfg.getMaxNumLogsInOneFile() * serializedLogSize;

      for (int i = 0; i < files.size() - 1; i++) {
        long fileSize = Files.size(files.get(i));
        if (fileSize != fileSizeSupposedToBe) {
          throw new IOException(
              "The file " + files.get(i) + " is not the last file and its size is less than "
                  + fileSizeSupposedToBe);
        }
      }

      if (files.size() >= 2) {
        Path latestCheckFile = files.get(files.size() - 2);
        try {
          getLogFileMetadataWithNoErrorTolerentable(latestCheckFile, segId);
        } catch (IOException e) {
          throw new BadLogFileException(latestCheckFile, "can't get log file metadata", 0);
        }
      }

      Path latestFile = files.get(files.size() - 1);
      try {
        LogStorageMetadata logStorageMetadata = getLogFileMetadataWithNoErrorTolerentable(
            latestFile, segId);

        logStorageMetadata.addLogFilesToTail(files);

        return new Pair<LogStorageMetadata, Path>(logStorageMetadata, latestFile);
      } catch (IOException e) {
        throw new BadLogFileException(latestFile, "can't get log file metadata", 0);
      }
    }
  }

  private LogStorageMetadata getLogFileMetadataWithErrorTolerentable(Path file, SegId segId)
      throws IOException {
    try {
      return getLogFileMetadata(file, true, segId);
    } catch (BadLogFileException e) {
      throw new IOException(
          "convert BadLogFileException to IOException because the function can tolerate file error",
          e);
    }
  }

  private LogStorageMetadata getLogFileMetadataWithNoErrorTolerentable(Path file, SegId segId)
      throws IOException, BadLogFileException {
    return getLogFileMetadata(file, false, segId);
  }

  private LogStorageMetadata getLogFileMetadata(Path file, boolean errorTolerentable, SegId segId)
      throws IOException, BadLogFileException {
    long latestFileSize = Files.size(file);
    if (latestFileSize < serializedLogSize) {
      String errMsg = "The last file " + file + " has size of " + latestFileSize
          + " less than the size of a single log " + serializedLogSize;
      logger.warn(errMsg);
      if (errorTolerentable) {
        return null;
      } else {
        throw new BadLogFileException(file, errMsg, 0);
      }
    }

    long remainingBytes = latestFileSize % serializedLogSize;
    long latestLogPos = latestFileSize - serializedLogSize;
    if (remainingBytes != 0) {
      String errMsg = "The last file " + file + " (" + latestFileSize
          + ") has a bad log which has less data at the end";
      logger.warn(errMsg);
      if (errorTolerentable) {
        latestLogPos -= remainingBytes;
      } else {
        throw new BadLogFileException(file, errMsg, latestFileSize - remainingBytes);
      }
    }

    MutationLogEntryReader reader = factory.generateReader();
    try {
      FileInputStream fileInputStream = new FileInputStream(file.toFile());

      long skipped = 0;
      while (skipped != latestLogPos) {
        skipped += fileInputStream.skip(latestLogPos - skipped);
      }

      reader.open(fileInputStream);
      MutationLogEntry lastEntryInFile = null;
      try {
        lastEntryInFile = reader.read();
      } catch (IOException e) {
        String errMsg = "Can't read the last log from " + file;
        logger.error(errMsg);
        throw new BadLogFileException(file, errMsg, latestLogPos);
      } catch (Exception e) {
        logger.error("catch an exception for segId {}, File {}", file, segId, e);
        String errMsg = "Can't read the last log from " + file;
        throw new BadLogFileException(file, errMsg, latestLogPos);
      }

      long maxLogId = lastEntryInFile.getLogId();
      int numLogs = (int) (latestFileSize / serializedLogSize);
      return new LogStorageMetadata(factory, cfg.getMaxNumLogsInOneFile(), numLogs, maxLogId,
          segId);
    } catch (IOException e) {
      String errMsg = "Caught an IOException. Can't get the latest log in the " + file
          + ". The file should be deleted";
      logger.error(errMsg);

      throw new BadLogFileException(file, errMsg, 0, e);
    } finally {
      if (reader != null) {
        reader.close();
      }
    }
  }

  private LogStorageMetadata createLogStorage(SegId segId, long id,
      LogStorageMetadata originalStorageMetadata)
      throws IOException {
    Files.createDirectories(getSegmentDirPath(segId));

    if (originalStorageMetadata == null) {
      originalStorageMetadata = new LogStorageMetadata(factory, cfg.getMaxNumLogsInOneFile(), 0, -1,
          segId);
    }

    Path newLogFile = generateLogFileObject(segId, id);
    MutationLogEntryWriter writer = factory.generateWriter();
    writer.open(new FileOutputStream(newLogFile.toFile(), false));

    originalStorageMetadata.addLogFileToTail(newLogFile);
    logger.debug("create new file to write:{}", newLogFile);
    originalStorageMetadata.setLogWriter(writer);
    synchronized (this) {
      this.logStorageFileCountQueue.remove(originalStorageMetadata);

      this.logStorageMetadatas.put(segId, originalStorageMetadata);
      this.logStorageFileCountQueue.add(originalStorageMetadata);

      boolean needToRemoveLogFile = false;
      boolean needToLogDown = false;

      long deleteCount = 0;
      if (++this.logFileCount > cfg.getMaxNumFilesInDataNode()) {
        needToLogDown = REMOVE_FILE_COUNTER.getAndIncrement() % 1000 == 0;
        if (needToLogDown) {
          logger.warn("total log file count {} exceeds threshold {}, remove oldest log file",
              logFileCount,
              cfg.getMaxNumFilesInDataNode());
        }
        needToRemoveLogFile = true;
        deleteCount = this.logFileCount - cfg.getMaxNumFilesInDataNode();
      } else if (originalStorageMetadata.getLogFileCount() > cfg.getMaxNumFilesInOneSegmentUnit()) {
        needToLogDown = needToLogDown || REMOVE_FILE_COUNTER.getAndIncrement() % 1000 == 0;
        if (needToLogDown) {
          logger.warn("log file count for seg {} : {} exceeds threshold {}, remove oldest log file",
              segId,
              originalStorageMetadata.getLogFileCount(), cfg.getMaxNumFilesInOneSegmentUnit());
        }
        needToRemoveLogFile = true;
        deleteCount =
            originalStorageMetadata.getLogFileCount() - cfg.getMaxNumFilesInOneSegmentUnit();
      }

      if (needToRemoveLogFile) {
        try {
          removeLogFile(deleteCount);
        } catch (Throwable t) {
          logger.warn("can't remove log file. count:{}", deleteCount, t);
        }
      }
    }

    return originalStorageMetadata;
  }

  private Path generateLogFileObject(SegId segId, long id) throws IOException {
    Path pathToLogFile = getLogFilePath(segId, id);
    try {
      Files.createFile(pathToLogFile);
    } catch (FileAlreadyExistsException e) {
      logger.warn("Log file {} exists. Do nothing", pathToLogFile);
    }

    return pathToLogFile;
  }

  private SegId parseSegmentIdFromDir(String dirName) {
    String[] strs = dirName.split("[._]");
    if (strs.length != 3) {
      throw new InvalidFormatException(dirName + " has a wrong format of log file");
    }

    try {
      return new SegId(Long.parseLong(strs[1]), Integer.parseInt(strs[2]));
    } catch (NumberFormatException e) {
      throw new InvalidFormatException(dirName + " has a wrong format", e);
    }
  }

  protected Path getLogFilePath(SegId segId, long id) {
    String strLogFileName = getLogFileName(segId, id);
    String segDirName = getSegmentDirName(segId);
    return FileSystems.getDefault().getPath(cfg.getRootDirName(), segDirName, strLogFileName);
  }

  protected Path getSegmentDirPath(SegId segId) {
    String segDirName = getSegmentDirName(segId);
    return FileSystems.getDefault().getPath(cfg.getRootDirName(), segDirName);
  }

  private void truncateFile(File f, long newSize) throws IOException {
    FileOutputStream fos = new FileOutputStream(f, true);
    FileChannel outChan = fos.getChannel();
    outChan.truncate(newSize);
    outChan.close();
    fos.close();
  }

  @Override
  public synchronized LogStorageMetadata createLogStorageWhenNoLogFileExist(SegId segId, long id)
      throws IOException {
    return createLogStorage(segId, id, null);
  }

  private void removeLogFile(long deleteCount) {
    for (int i = 0; i < deleteCount; i++) {
      LogStorageMetadata logStorageMetadata = this.logStorageFileCountQueue.poll();
      logger.info("before remove logfile, log storage metadata {}", logStorageMetadata);
      Validate.notNull(logStorageMetadata);
      logStorageMetadata.removeLogFileAtHead();
      this.logFileCount--;
      logger.info("after remove logfile, log storage metadata {}", logStorageMetadata);

      this.logStorageFileCountQueue.add(logStorageMetadata);
    }
  }

  @Override
  public synchronized void close() {
    for (Entry<SegId, LogStorageMetadata> entry : this.logStorageMetadatas.entrySet()) {
      LogStorageMetadata logStorageMetadata = entry.getValue();
      if (logStorageMetadata != null) {
        logStorageMetadata.close();
      }
    }

    this.logStorageFileCountQueue.clear();
    this.logFileCount = 0;
  }

  public synchronized boolean initLogSystemForSeg(SegId segId) {
    LogStorageMetadata logStorageMetadata = this.logStorageMetadatas.remove(segId);
    if (logStorageMetadata != null) {
      logStorageMetadata.close();
      this.logStorageFileCountQueue.remove(logStorageMetadata);
      this.logFileCount -= logStorageMetadata.getLogFileCount();
    }

    Path segDir = getSegmentDirPath(segId);
    while (true) {
      try {
        Pair<LogStorageMetadata, Path> storageInfo = initSegment(segDir, segId);
        if (storageInfo == null) {
          logger.warn("throw an IO exception, so that the directory {} can be deleted", segDir);
          throw new IOException();
        }

        LogStorageMetadata logMetadata = storageInfo.getFirst();
        MutationLogEntryWriter writer = null;
        if (logMetadata.moreSpace()) {
          writer = factory.generateWriter();
          writer.open(new FileOutputStream(storageInfo.getSecond().toFile(), true));
          logMetadata.setLogWriter(writer);
        }

        logStorageMetadatas.put(segId, logMetadata);
        logStorageFileCountQueue.add(logMetadata);
        logFileCount += logMetadata.getLogFileCount();

        break;
      } catch (BadLogFileException e) {
        logger.error("failed to initialize {}", segDir, e);

        logger.warn("deleting file: {}", e.getBadFile());
        try {
          Files.delete(e.getBadFile());
        } catch (IOException ioe) {
          logger.warn("faile to delete dir", ioe);
        }
      } catch (IOException e) {
        logger.error("Can't iterator the dir. delete the dir", e);

        try {
          FileUtils.deleteDirectory(segDir.toFile());
        } catch (Exception ioe) {
          logger.warn("fail to delete dir", ioe);
        }
        return false;
      }

    }

    return true;
  }
}
