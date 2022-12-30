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

package py.datanode.segment.datalog.backup;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.util.List;
import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.segment.SegId;
import py.datanode.segment.datalog.LogImage;
import py.datanode.segment.datalog.MutationLogEntry;
import py.datanode.segment.datalog.MutationLogEntryFactory;
import py.datanode.segment.datalog.SegmentLogMetadata;
import py.exception.NoAvailableBufferException;

public class PlalLogSaver implements MutationLogBackup {
  private static final Logger logger = LoggerFactory.getLogger(PlalLogSaver.class);
  private static final int LOG_ENTRY_HEADER_LEN =
      8 + 8 + 4 + 8 + 4 + 8 + 4 + 8 + 8 + 4;

  private ByteBuffer headBuffer;
  private String backupFileName;
  private RandomAccessFile raf;
  private boolean readHasBegin = false;
  private String rootDir;

  public PlalLogSaver(SegId segId, String rootDir) {
    this.rootDir = rootDir + "/";
    this.backupFileName = segId.getVolumeId() + "-" + segId.getIndex() + "_backuplog";
    headBuffer = ByteBuffer.allocate(LOG_ENTRY_HEADER_LEN);
  }

  @Override
  public void backupMutationLog(SegmentLogMetadata logMetadata) throws Exception {
    long plalId = logMetadata.getLalId();
    long clId = logMetadata.getClId();
    long writeDataLen = 0;
    long clLogOffsetInFile = 0;
    if (plalId == LogImage.INVALID_LOG_ID) {
      return;
    }

    List<MutationLogEntry> logs = logMetadata.getLogsAfter(plalId, Integer.MAX_VALUE);
    raf.writeLong(0);
    writeDataLen += 8;

    for (MutationLogEntry log : logs) {
      writeDataLen += writeMutationLogEntry(log);
      if (log.getLogId() == clId) {
        clLogOffsetInFile = writeDataLen;
        logger.warn("the CL log offset in file is {}", clLogOffsetInFile);
      }
    }

    raf.seek(0);
    raf.writeLong(clLogOffsetInFile);
    logger.debug("cl log 's offset in file is {}", clLogOffsetInFile);
  }

  private long writeMutationLogEntry(MutationLogEntry log) throws IOException {
    logger.debug("back up log {} at offset {}", log, raf.getFilePointer());
    Validate.notNull(log.getData());
    Validate.isTrue(log.getLength() == log.getData().length);

    long length = 0;
    ByteBuffer head = getHeadBufferForLog(log);
    raf.write(head.array());
    length += LOG_ENTRY_HEADER_LEN;
    raf.write(log.getData());
    length += log.getLength();
    return length;
  }

  private ByteBuffer getHeadBufferForLog(MutationLogEntry log) {
    this.headBuffer.clear();
    headBuffer.putLong(log.getUuid());
    headBuffer.putLong(log.getLogId());

    headBuffer.putInt(getLogStatusFieldValue(log.getStatus()));
    headBuffer.putLong(log.getOffset());
    headBuffer.putInt(log.getLength());
    headBuffer.putLong(log.getChecksum());

    return headBuffer;
  }

  private int getLogStatusFieldValue(MutationLogEntry.LogStatus status) {
    if (status == MutationLogEntry.LogStatus.Committed) {
      return 0x1;
    } else if (status == MutationLogEntry.LogStatus.AbortedConfirmed) {
      return 0x2;
    } else if (status == MutationLogEntry.LogStatus.Created) {
      return 0x3;
    } else if (status == MutationLogEntry.LogStatus.Aborted) {
      return 0x4;
    }

    Validate.isTrue(false);
    return 0;
  }

  private MutationLogEntry.LogStatus getLogStatusByFieldValue(int fieldValue) {
    if (1 > fieldValue || fieldValue > 4) {
      throw new IllegalArgumentException(String.format("field value is illegal %d", fieldValue));
    }
    if (fieldValue == 1) {
      return MutationLogEntry.LogStatus.Committed;
    } else if (fieldValue == 2) {
      return MutationLogEntry.LogStatus.AbortedConfirmed;
    } else if (fieldValue == 3) {
      return MutationLogEntry.LogStatus.Created;
    } else {
      return MutationLogEntry.LogStatus.Aborted;
    }
  }

  @Override
  public void deleteBackup() {
    File backupFile = new File(this.rootDir, this.backupFileName);
    backupFile.delete();
  }

  @Override
  public boolean readMutationLogs(List<MutationLogEntry> logs, int maxLogNum)
      throws NoAvailableBufferException, IOException {
    if (!readHasBegin) {
      raf.skipBytes(8);
      readHasBegin = true;
    }

    return readMutationLog(logs, maxLogNum);
  }

  @Override
  public void open() throws IOException {
    try {
      Files.createDirectories(FileSystems.getDefault().getPath(this.rootDir));
    } catch (FileAlreadyExistsException e) {
      logger.info("{} exists. Don't need to create the directory for saving logs", this.rootDir);
    }

    File backupFile = new File(this.rootDir, backupFileName);
    logger.warn("open file {} for backup log and root {}", backupFileName, this.rootDir);
    if (!backupFile.exists()) {
      backupFile.createNewFile();
    }

    raf = new RandomAccessFile(rootDir + backupFileName, "rw");
  }

  @Override
  public void close() throws IOException {
    raf.close();
  }

  @Override
  public boolean readMutationLogsAfterPcl(List<MutationLogEntry> logs, int maxLogNum)
      throws NoAvailableBufferException, IOException {
    long clOffset = 0;

    if (!readHasBegin) {
      clOffset = raf.readLong();
      logger.debug("first log after PCL is {}", clOffset);
      raf.seek(clOffset);
      readHasBegin = true;
    }

    return readMutationLog(logs, maxLogNum);
  }

  private boolean readMutationLog(List<MutationLogEntry> logs, int maxLogNumber)
      throws IOException, NoAvailableBufferException {
    int logNum = 0;
    long currentPosition = raf.getFilePointer();
    long fileLength = raf.length();

    while (logNum < maxLogNumber && currentPosition < fileLength) {
      long preOffset = currentPosition;
      logger.debug("current position {}, fileLength {}", currentPosition, fileLength);

      this.headBuffer.clear();
      int readLength = raf.read(headBuffer.array());
      if (readLength != LOG_ENTRY_HEADER_LEN) {
        logger.error("read header length {} is not our expected {}", readLength,
            LOG_ENTRY_HEADER_LEN);
        throw new IOException(
            "mutation header length " + readLength + " is not our expected "
                + LOG_ENTRY_HEADER_LEN);
      }

      long uuid = headBuffer.getLong();
      long logId = headBuffer.getLong();

      MutationLogEntry.LogStatus status = getLogStatusByFieldValue(headBuffer.getInt());
      long offset = headBuffer.getLong();
      int length = headBuffer.getInt();
      long checksum = headBuffer.getLong();
      int snapshotVersion = headBuffer.getInt();
      long srcOffset = headBuffer.getLong();
      int srcLength = headBuffer.getInt();

      byte[] dataArrayForLog = new byte[length];
      readLength = raf.read(dataArrayForLog, 0, length);
      if (readLength != length) {
        logger.error("read data length {} is not our expected {}", readLength, length);
        throw new IOException(
            "mutation data length " + readLength + " is not our expected " + length);
      }

      currentPosition += LOG_ENTRY_HEADER_LEN;
      currentPosition += length;

      try {
        MutationLogEntry entry = MutationLogEntryFactory
            .createLogForPrimary(uuid, logId, 0L, offset,
                dataArrayForLog, checksum);
        entry.setStatus(status);
        logger.debug("get log entry {}", entry);
        logs.add(entry);
        logNum++;
      } catch (NoAvailableBufferException e) {
        logger.warn("can not all allocate fastbuffer for data size {}", length);
        raf.seek(preOffset);
        throw e;
      }
    }

    logger.debug("after read, currentPosition {}, fileLength {}", currentPosition, fileLength);
    return currentPosition >= fileLength;
  }

}
