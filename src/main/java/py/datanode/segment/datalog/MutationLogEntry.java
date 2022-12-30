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

import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import io.netty.buffer.ByteBuf;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.FastBuffer;
import py.common.HeapIndependentObject;
import py.datanode.exception.GenericPageSystemException;
import py.datanode.exception.InappropriateLogStatusException;
import py.datanode.exception.IncorrectRangeException;
import py.datanode.page.Page;
import py.exception.StorageException;
import py.proto.Broadcastlog;
import py.proto.Broadcastlog.PbBroadcastLogStatus;
import py.thrift.share.BroadcastLogStatusThrift;
import py.thrift.share.BroadcastLogThrift;

public class MutationLogEntry extends HeapIndependentObject implements
    Comparable<MutationLogEntry> {
  public static final Logger logger = LoggerFactory.getLogger(MutationLogEntry.class);
  public static MutationLogEntry INVALID_MUTATION_LOG;
  public static MutationLogEntry LAZY_CLONE_INIT_LOG;

  public static Long DEFAULT_ARCHIVE_ID = 0L;
  public static Long INVALID_CLONE_VOLUME_OFFSET = 0L;
  public static Integer INVALID_CLONE_VOLUME_LENGTH = 0;

  static {
    INVALID_MUTATION_LOG = new MutationLogEntry(LogImage.INVALID_LOG_ID,
        LogImage.INVALID_LOG_ID, 0L, 0L, null, 0L);
    INVALID_MUTATION_LOG.commitAndApply();
    INVALID_MUTATION_LOG.setPersisted();

    LAZY_CLONE_INIT_LOG = new MutationLogEntry(LogImage.INVALID_LOG_ID + 1,
        LogImage.INVALID_LOG_ID + 1, 0L, 0L,
        null, 0L);
    LAZY_CLONE_INIT_LOG.commitAndApply();
    LAZY_CLONE_INIT_LOG.setPersisted();
  }

  private volatile long uuid;
  private volatile long logId;
  // the offset in a segment where a change starts to apply to
  private volatile long offset;
  // When the data array is released, we still have to remember the length of
  // the data array. Therefore, it is necessary to have a length variable
  private volatile int dataLength;
  private volatile long checksum;
  private volatile LogStatus status;
  private volatile boolean applied;
  // inserted means whether the log in a secondary has been inserted into SegmentLogMetadata,
  // currently, it's only  used for debug purpose
  private volatile boolean inserted;
  private volatile boolean persisted;
  // For dummy mutation log entry, just include log id
  private volatile boolean dummy;

  // This is only valid in primary. When the log has been expired, we will finish it.
  private long expirationTimeMs;
  //private final LogDataHolderIface dataHolder;
  private LogDataContainer logDataContainer;
  private Long archiveId;

  public MutationLogEntry(long uuid, long logId, Long archiveId, long offset, FastBuffer dataBuf,
      long checksum) {
    this.uuid = uuid;
    this.logId = logId;
    this.offset = offset;

    this.checksum = checksum;
    this.status = LogStatus.Created;
    this.applied = false;
    this.persisted = false;
    if (dataBuf != null) {
      this.logDataContainer = new LogDataContainerFastBuffer(dataBuf);
      this.dataLength = (int) dataBuf.size();
    }
    this.archiveId = archiveId;

    allocate();
  }

  public boolean isFinalStatus() {
    return isFinalStatus(status);
  }
  
  public static boolean isFinalStatus(LogStatus logStatus) {
    return logStatus == LogStatus.Committed || logStatus == LogStatus.AbortedConfirmed;
  }

  public static BroadcastLogThrift buildBroadcastLogFrom(MutationLogEntry log) {
    BroadcastLogThrift logThrift = new BroadcastLogThrift();
    logThrift.setLogId(log.getLogId());
    logThrift.setChecksum(log.getChecksum());
    logThrift.setOffset(log.getOffset());
    logThrift.setLength(log.getLength());
    logThrift.setLogStatus(convertStatusToThriftStatus(log.getStatus()));
    return logThrift;
  }

  public static BroadcastLogStatusThrift convertStatusToThriftStatus(LogStatus status) {
    BroadcastLogStatusThrift logThrift = null;
    if (status == LogStatus.Created) {
      logThrift = BroadcastLogStatusThrift.Created;
    } else if (status == LogStatus.Committed) {
      logThrift = BroadcastLogStatusThrift.Committed;
    } else if (status == LogStatus.Aborted) {
      logThrift = BroadcastLogStatusThrift.Abort;
    } else if (status == LogStatus.AbortedConfirmed) {
      logThrift = BroadcastLogStatusThrift.AbortConfirmed;
    } else {
      logger.error("unknow status: {}", status);
    }
    return logThrift;
  }

  public FastBuffer getFastBuffer() {
    return logDataContainer == null ? null : logDataContainer.getFastBuffer();
  }

  public long getUuid() {
    return uuid;
  }

  public long getLogId() {
    return logId;
  }

  public void setLogId(long logId) {
    this.logId = logId;
  }

  public void put(ByteBuf byteBuf) {
    Validate.isTrue(logDataContainer.getFastBuffer() != null);

    FastBuffer fastBuffer = logDataContainer.getFastBuffer();

    if (byteBuf.nioBufferCount() == 1) {
      fastBuffer.put(byteBuf.nioBuffer());
    } else {
      int dstOffset = 0;
      for (ByteBuffer buffer : byteBuf.nioBuffers()) {
        int remaining = buffer.remaining();
        fastBuffer.put(dstOffset, buffer, buffer.position(), remaining);
        dstOffset += remaining;
      }
    }
  }

  public LogStatus getStatus() {
    return status;
  }

  public void setStatus(LogStatus newStatus) throws InappropriateLogStatusException {
    if (newStatus == null || (this.status == LogStatus.AbortedConfirmed && (newStatus
        != LogStatus.AbortedConfirmed)) || (this.status == LogStatus.Committed && (newStatus
        != LogStatus.Committed))) {
      throw new InappropriateLogStatusException(
          "logId: " + logId + " Can not set log status to [" + newStatus
              + "] because my current status "
              + this.status + " is the final status");
    }

    this.status = newStatus;
  }

  public long getOffset() {
    return offset;
  }

  public void insert() {
    this.inserted = true;
  }

  public void commit() throws InappropriateLogStatusException {
    setStatus(LogStatus.Committed);
  }

  public void abort() throws InappropriateLogStatusException {
    setStatus(LogStatus.Aborted);
  }

  public void confirmAbortAndApply() throws InappropriateLogStatusException {
    apply();
    setStatus(LogStatus.AbortedConfirmed);
  }

  public void confirmAbort() throws InappropriateLogStatusException {
    setStatus(LogStatus.AbortedConfirmed);
  }

  public void apply() {
    applied = true;
  }

  public void resetNotApply() {
    applied = false;
  }

  public void commitAndApply() throws InappropriateLogStatusException {
    apply();
    commit();
  }

  public boolean isApplied() {
    return applied;
  }

  public boolean isPersisted() {
    return persisted;
  }

  public boolean isCommitted() {
    return status == LogStatus.Committed;
  }

  @Override
  public void setPersisted() {
    if (!(isApplied() && (status == LogStatus.Committed || status == LogStatus.AbortedConfirmed))) {
      throw new InappropriateLogStatusException("Log " + this.toString() + " can't be persisted ");
    }

    this.persisted = true;
  }

  public long getChecksum() {
    return checksum;
  }

  public void setChecksum(long checksum) {
    this.checksum = checksum;
  }

  public long getExpirationTimeMs() {
    return expirationTimeMs;
  }

  public void setExpirationTimeMs(long expirationTimeMs) {
    this.expirationTimeMs = expirationTimeMs;
  }

  public boolean isExpired() {
    if (expirationTimeMs == 0) {
      return true;
    }
    return System.currentTimeMillis() > this.expirationTimeMs;
  }

  public int getLength() {
    return dataLength;
  }

  public void setLength(int length) {
    this.dataLength = length;
  }

  @Override
  public int compareTo(MutationLogEntry o) {
    if (o == null) {
      return 1;
    } else {
      long diff = (logId - o.getLogId());
      if (diff > 0) {
        return 1;
      } else if (diff < 0) {
        return -1;
      } else {
        long uuidDiff = uuid - o.getUuid();
        if (uuidDiff > 0) {
          return 1;
        } else if (uuidDiff < 0) {
          return -1;
        } else {
          return 0;
        }
      }
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof MutationLogEntry)) {
      return false;
    }

    MutationLogEntry that = (MutationLogEntry) o;

    if (uuid != that.uuid) {
      return false;
    }
    return logId == that.logId;
  }

  @Override
  public int hashCode() {
    int result = (int) (uuid ^ (uuid >>> 32));
    result = 31 * result + (int) (logId ^ (logId >>> 32));
    return result;
  }

  public synchronized void applyLogData(Page page, int pageSize, RangeSet<Integer> rangesToApply)
      throws IncorrectRangeException, GenericPageSystemException {
    Validate.isTrue(!applied);
    if (status != LogStatus.Committed) {
      Validate.isTrue(false, this.toString());
    }

    if (!(dataLength <= pageSize && dataLength > 0)) {
      Validate.isTrue(false, "mutation log: " + this.toString() + ", pageSize: " + pageSize);
    }

    FastBuffer data = logDataContainer.getFastBuffer();

    if (data == null) {
      Validate.isTrue(false, "data buffer is null " + this.toString());
    }

    if (data.size() != dataLength) {
      Validate.isTrue(false, "mutation log: " + this.toString() + ", size: " + data.size());
    }

    page.write((int) (offset % pageSize), data, rangesToApply);
  }

  public synchronized void applyLogData(byte[] destination, int offsetAtPage, int length,
      int pageSize) {
    if (status == LogStatus.AbortedConfirmed) {
      return;
    }

    if (!(dataLength <= pageSize && dataLength > 0)) {
      Validate.isTrue(false,
          "dataLength: " + dataLength + ", length: " + length + ", pageSize: " + pageSize);
    }

    if (!hasData()) {
      Validate.isTrue(false, this.toString());
    }

    Range<Integer> rangeOfDataLog = Range.closed(0, dataLength - 1);
    applyLogData(destination, offsetAtPage, length, pageSize, rangeOfDataLog);
  }

  public synchronized void applyLogData(byte[] destination, int offsetAtPage, int length,
      int pageSize,
      RangeSet<Integer> rangesToApply)
      throws IncorrectRangeException, StorageException, GenericPageSystemException {
    Validate.isTrue(!applied);
    if (status != LogStatus.Committed && status != LogStatus.AbortedConfirmed) {
      Validate.isTrue(false, this.toString());
    }

    if (status == LogStatus.AbortedConfirmed) {
      return;
    }

    if (!(dataLength <= pageSize && dataLength > 0)) {
      Validate.isTrue(false,
          "dataLength: " + dataLength + ", length: " + length + ", pageSize: " + pageSize);
    }

    if (!hasData()) {
      Validate.isTrue(false, this.toString());
    }

    int fastBufferDataLen = (int) logDataContainer.getDataLength();
    if (fastBufferDataLen != dataLength) {
      Validate.isTrue(false, "mutation log: " + this.toString() + ", size: " + fastBufferDataLen);
    }

    if (rangesToApply == null || rangesToApply.isEmpty() || rangesToApply.asRanges().size() == 0) {
      logger.debug("no log to apply");
      return;
    }
    for (Range<Integer> range : rangesToApply.asRanges()) {
      applyLogData(destination, offsetAtPage, length, pageSize, range);
    }
  }

  public synchronized void applyLogData(ByteBuf destination, int offsetAtPage, int length,
      int pageSize,
      RangeSet<Integer> rangesToApply)
      throws IncorrectRangeException, StorageException, GenericPageSystemException {
    Validate.isTrue(!applied);
    if (status != LogStatus.Committed && status != LogStatus.AbortedConfirmed) {
      Validate.isTrue(false, this.toString());
    }

    if (status == LogStatus.AbortedConfirmed) {
      return;
    }

    if (!(dataLength <= pageSize && dataLength > 0)) {
      Validate.isTrue(false,
          "dataLength: " + dataLength + ", length: " + length + ", pageSize: " + pageSize);
    }

    if (!hasData()) {
      Validate.isTrue(false, this.toString());
    }

    int fastBufferDataLen = (int) logDataContainer.getDataLength();
    if (fastBufferDataLen != dataLength) {
      Validate.isTrue(false, "mutation log: " + this.toString() + ", size: " + fastBufferDataLen);
    }

    if (rangesToApply == null || rangesToApply.isEmpty() || rangesToApply.asRanges().size() == 0) {
      logger.debug("no log to apply");
      return;
    }
    for (Range<Integer> range : rangesToApply.asRanges()) {
      applyLogData(destination, offsetAtPage, length, pageSize, range);
    }
  }

  public synchronized void applyLogData(ByteBuf destination, int offsetAtPage, int length,
      int pageSize,
      Range<Integer> rangeOfDataLog) {
    logger
        .debug("offsetAtPage:{} length:{} rangeOfDataLog:{}", offsetAtPage, length, rangeOfDataLog);
    if (rangeOfDataLog.isEmpty()) {
      return;
    }

    try {
      int logOffsetAtPage = (int) (offset % pageSize);

      Range<Integer> rangeOfReturnedData = Range
          .closed(offsetAtPage - logOffsetAtPage, offsetAtPage - logOffsetAtPage + length - 1);
      logger.debug("rangeOfReturnedData: {}", rangeOfReturnedData);
      Range<Integer> interaction;
      try {
        interaction = rangeOfDataLog.intersection(rangeOfReturnedData);
      } catch (IllegalArgumentException e) {
        logger.warn("no overlap, {}, {}", rangeOfDataLog, rangeOfReturnedData);
        return;
      }
      int lowerEndPoint = interaction.lowerEndpoint();
      int upperEndPoint = interaction.upperEndpoint();

      if (interaction.upperBoundType() == BoundType.OPEN) {
        upperEndPoint--;
      }
      if (interaction.lowerBoundType() == BoundType.OPEN) {
        lowerEndPoint++;
      }

      logger.debug("interaction : {}, lowerEndPoint:{}, upperEndPointer:{} ", interaction,
          lowerEndPoint,
          upperEndPoint);
      int compositeCount = destination.nioBufferCount();
      FastBuffer data = logDataContainer.getFastBuffer();
      if (compositeCount == 1) {
        data.get(lowerEndPoint, destination.nioBuffer(),
            lowerEndPoint - rangeOfReturnedData.lowerEndpoint(),
            upperEndPoint - lowerEndPoint + 1);
      } else {
        int offsetInSrc = lowerEndPoint;
        int offsetInDes = lowerEndPoint - rangeOfReturnedData.lowerEndpoint();
        int leftLength = upperEndPoint - lowerEndPoint + 1;

        for (ByteBuffer buffer : destination.nioBuffers()) {
          if (leftLength == 0) {
            break;
          }

          int currentLength = buffer.remaining();

          if (offsetInDes != 0) {
            if (currentLength <= offsetInDes) {
              offsetInDes -= currentLength;
              continue;
            } else {
              currentLength -= offsetInDes;
              currentLength = (currentLength <= leftLength ? currentLength : leftLength);

              data.get(offsetInSrc, buffer, offsetInDes, currentLength);
              leftLength -= currentLength;
              offsetInDes = 0;
              offsetInSrc += currentLength;
              continue;
            }
          }

          currentLength = (currentLength <= leftLength ? currentLength : leftLength);
          data.get(offsetInSrc, buffer, offsetInDes, currentLength);
          offsetInSrc += currentLength;
          offsetInDes = 0;
          leftLength -= currentLength;
        }
      }

      if (data == null) {
        Validate.isTrue(false, this.toString());
      }
    } catch (IllegalArgumentException e) {
      logger.debug("no overlap", e);
    }
  }

  public synchronized void applyLogData(byte[] destination, int offsetAtPage, int length,
      int pageSize,
      Range<Integer> rangeOfDataLog) {
    logger
        .debug("offsetAtPage:{} length:{} rangeOfDataLog:{}", offsetAtPage, length, rangeOfDataLog);
    if (rangeOfDataLog.isEmpty()) {
      return;
    }

    try {
      int logOffsetAtPage = (int) (offset % pageSize);

      Range<Integer> rangeOfReturnedData = Range
          .closed(offsetAtPage - logOffsetAtPage, offsetAtPage - logOffsetAtPage + length - 1);
      logger.debug("rangeOfReturnedData: {}", rangeOfReturnedData);
      Range<Integer> interaction;
      try {
        interaction = rangeOfDataLog.intersection(rangeOfReturnedData);
      } catch (IllegalArgumentException e) {
        logger.warn("no overlap, {}, {}", rangeOfDataLog, rangeOfReturnedData);
        return;
      }

      int lowerEndPoint = interaction.lowerEndpoint();
      int upperEndPoint = interaction.upperEndpoint();

      if (interaction.upperBoundType() == BoundType.OPEN) {
        upperEndPoint--;
      }
      if (interaction.lowerBoundType() == BoundType.OPEN) {
        lowerEndPoint++;
      }

      logger.debug("interaction : {}, lowerEndPoint:{}, upperEndPointer:{} ", interaction,
          lowerEndPoint,
          upperEndPoint);

      FastBuffer data = logDataContainer.getFastBuffer();
      data.get(lowerEndPoint, destination, lowerEndPoint - rangeOfReturnedData.lowerEndpoint(),
          upperEndPoint - lowerEndPoint + 1);
      logger.debug("bufferToReturn:{}", Arrays.toString(destination));
    } catch (IllegalArgumentException e) {
      logger.debug("no overlap", e);
    }
  }

  synchronized FastBuffer releaseBuffer() {
    return logDataContainer == null ? null : logDataContainer.releaseBuffer();
  }

  public synchronized byte[] getData() {
    if (logDataContainer != null) {
      return logDataContainer.getArrayData();
    } else {
      return null;
    }
  }

  public boolean hasData() {
    if (logDataContainer == null) {
      return false;
    }
    return logDataContainer.getDataLength() > 0;
  }

  @Override
  public String toString() {
    return "MutationLogEntry " + "[uuid=" + uuid + ", id=" + logId + ", offset=" + offset
        + ", length=" + dataLength
        + ", checksum=" + checksum + ", status=" + status + ", inserted=" + inserted
        + ", applied=" + applied + ", persisted=" + persisted
        + ", expirationTime=" + expirationTimeMs
        + ", logDataContainer=" + logDataContainer + "]";
  }

  public boolean isDummy() {
    return dummy;
  }

  public void setDummy(boolean dummy) {
    this.dummy = dummy;
  }

  public boolean canBeApplied() {
    return status == LogStatus.Committed && !applied;
  }

  public boolean isAbort() {
    return status == LogStatus.Aborted || status == LogStatus.AbortedConfirmed;
  }

  public Long getArchiveId() {
    return archiveId;
  }

  public enum LogStatus {
    Created {
      @Override
      public Broadcastlog.PbBroadcastLogStatus getPbLogStatus() {
        return Broadcastlog.PbBroadcastLogStatus.CREATED;
      }
    },

    Aborted {
      @Override
      public Broadcastlog.PbBroadcastLogStatus getPbLogStatus() {
        return Broadcastlog.PbBroadcastLogStatus.ABORT;
      }
    }, 

    AbortedConfirmed {
      @Override
      public Broadcastlog.PbBroadcastLogStatus getPbLogStatus() {
        return Broadcastlog.PbBroadcastLogStatus.ABORT_CONFIRMED;
      }
    },

    Committed {
      @Override
      public Broadcastlog.PbBroadcastLogStatus getPbLogStatus() {
        return Broadcastlog.PbBroadcastLogStatus.COMMITTED;
      }
    };

    public static LogStatus findByValue(String value) {
      if (value == null) {
        return null;
      }

      if (value.equals("Created")) {
        return Created;
      } else if (value.equals("Aborted")) {
        return Aborted;
      } else if (value.equals("AbortedConfirmed")) {
        return AbortedConfirmed;
      } else if (value.equals("Committed")) {
        return Committed;
      } else {
        return null;
      }
    }

    public static LogStatus convertPbStatusToLogStatus(PbBroadcastLogStatus pbBroadcastLogStatus) {
      LogStatus convertStatus = null;
      Validate.notNull(pbBroadcastLogStatus, "can not be null status");
      if (pbBroadcastLogStatus == PbBroadcastLogStatus.COMMITTED) {
        convertStatus = LogStatus.Committed;
      } else if (pbBroadcastLogStatus == PbBroadcastLogStatus.CREATED) {
        convertStatus = LogStatus.Created;
      } else if (pbBroadcastLogStatus == PbBroadcastLogStatus.CREATING) {
        convertStatus = LogStatus.Created;
      } else if (pbBroadcastLogStatus == PbBroadcastLogStatus.ABORT) {
        convertStatus = LogStatus.Aborted;
      } else if (pbBroadcastLogStatus == PbBroadcastLogStatus.ABORT_CONFIRMED) {
        convertStatus = LogStatus.AbortedConfirmed;
      } else {
        logger.warn("don't know this status: {}", pbBroadcastLogStatus.name());
      }
      return convertStatus;
    }

    public Broadcastlog.PbBroadcastLogStatus getPbLogStatus() {
      throw new NotImplementedException("");
    }
  }

  private interface LogDataHolderIface {
    FastBuffer releaseBuffer();

    boolean hasData();

    byte[] getArrayData();

    FastBuffer getFastBufferData();

    int getFastBufferDataLen();

    void setDataBuf(FastBuffer dataBuf);

    boolean hasDataBuf();
  }

  private interface LogDataContainer {
    FastBuffer releaseBuffer();

    FastBuffer getFastBuffer();

    long getDataLength();

    byte[] getArrayData();
  }

  private class LogDataHolder implements LogDataHolderIface {
    private volatile FastBuffer dataBuf = null;

    public LogDataHolder() {
    }

    public LogDataHolder(FastBuffer dataBuf) {
      this.dataBuf = dataBuf;
    }

    @Override
    public FastBuffer releaseBuffer() {
      FastBuffer tempBuf = dataBuf;
      dataBuf = null;
      return tempBuf;
    }

    @Override
    public byte[] getArrayData() {
      if (dataBuf != null) {
        return dataBuf.array();
      } else {
        return null;
      }
    }

    @Override
    public void setDataBuf(FastBuffer dataBuf) {
      this.dataBuf = dataBuf;
    }

    @Override
    public boolean hasData() {
      return (dataBuf != null);
    }

    @Override
    public FastBuffer getFastBufferData() {
      return dataBuf;
    }

    @Override
    public int getFastBufferDataLen() {
      return (int) dataBuf.size();
    }

    @Override
    public boolean hasDataBuf() {
      return dataBuf != null;
    }

    @Override
    public String toString() {
      return "LogDataHolder{" + "dataBuf=" + dataBuf + '}';
    }
  }

  private class LogDataContainerFastBuffer implements LogDataContainer {
    FastBuffer fastBuffer;

    public LogDataContainerFastBuffer(FastBuffer fastBuffer) {
      this.fastBuffer = fastBuffer;
    }

    @Override
    public FastBuffer releaseBuffer() {
      if (fastBuffer == null) {
        return null;
      }
      FastBuffer tmp = fastBuffer;
      fastBuffer = null;
      return tmp;
    }

    @Override
    public FastBuffer getFastBuffer() {
      return fastBuffer;
    }

    @Override
    public long getDataLength() {
      return (fastBuffer == null ? 0 : fastBuffer.size());
    }

    @Override
    public byte[] getArrayData() {
      if (fastBuffer != null) {
        return fastBuffer.array();
      } else {
        return null;
      }
    }

    @Override
    public String toString() {
      return "LogDataContainerFastBuffer{" + "fastBuffer=" + getDataLength() + '}';
    }
  }

}
