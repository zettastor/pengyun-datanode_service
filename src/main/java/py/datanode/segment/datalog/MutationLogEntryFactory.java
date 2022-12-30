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

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.FastBuffer;
import py.common.FastBufferManagerProxy;
import py.common.HeapIndependentObject;
import py.common.HeapMemoryFastBufferImpl;
import py.common.LogPoolType;
import py.common.PrimaryFastBufferImpl;
import py.common.SyncLogFastBufferImpl;
import py.common.TlsfFileBufferManager;
import py.common.array.WtsHeapFastBufferImpl;
import py.datanode.configuration.DataNodeConfiguration;
import py.datanode.segment.datalog.MutationLogEntry.LogStatus;
import py.exception.NoAvailableBufferException;

public class MutationLogEntryFactory {
  private static final Logger logger = LoggerFactory.getLogger(MutationLogEntryFactory.class);
  protected static DataNodeConfiguration cfg;
  private static Map<Long, FastBufferManagerProxy> archiveIdToFastBufferManagerMap;
  private static Map<Long, FastBufferManagerProxy> archiveIdToSecondFastBufferManagerMap;
  private static FastBufferManagerProxy syncLogFastBufferManager;
  
  private static HeapMemoryFastBufferManager<HeapMemoryFastBufferImpl> membufferList;
  private static FastBufferManagerProxy fileFastBufferManager;

  public static void init(FastBufferManagerProxy primaryManager,
      FastBufferManagerProxy fileBufferManager,
      FastBufferManagerProxy secondManager, FastBufferManagerProxy syncLogManager,
      DataNodeConfiguration cfg)
      throws Exception {
    MutationLogEntryFactory.cfg = cfg;
    archiveIdToFastBufferManagerMap = new ConcurrentHashMap<>();
    archiveIdToSecondFastBufferManagerMap = new ConcurrentHashMap<>();

    if (primaryManager != null) {
      archiveIdToFastBufferManagerMap.put(MutationLogEntry.DEFAULT_ARCHIVE_ID, primaryManager);
    }
    if (secondManager != null) {
      archiveIdToSecondFastBufferManagerMap.put(MutationLogEntry.DEFAULT_ARCHIVE_ID, secondManager);
    }
    syncLogFastBufferManager = syncLogManager;
    fileFastBufferManager = fileBufferManager;

    Collection<HeapMemoryFastBufferImpl> extraHeapBuffers = new ArrayList<>(
        cfg.getNumberOfBuffersForMissingLog());
    int i = 0;
    while (i < cfg.getNumberOfBuffersForMissingLog()) {
      extraHeapBuffers.add(new HeapMemoryFastBufferImplExtended(new byte[cfg.getPageSize()]));
      i++;
    }
    membufferList = new HeapMemoryFastBufferManager<HeapMemoryFastBufferImpl>(extraHeapBuffers);
  }

  public static void addFastBufferManager(Long archiveId,
      FastBufferManagerProxy fastBufferManager) {
    if (archiveIdToFastBufferManagerMap.get(archiveId) == null) {
      archiveIdToFastBufferManagerMap.put(archiveId, fastBufferManager);
      archiveIdToSecondFastBufferManagerMap.put(archiveId, fastBufferManager);
    }
  }

  public static void deleteFastBufferManager(Long archiveId) {
    archiveIdToFastBufferManagerMap.remove(archiveId);
    archiveIdToSecondFastBufferManagerMap.remove(archiveId);
  }

  private static MutationLogEntry createLog(long uuid, long logId, Long archiveId, long offset,
      int length, long checksum, LogPoolType logPoolType, int timeout)
      throws NoAvailableBufferException {
    if (logId == -1 || offset == -1) {
      Validate.isTrue(false,
          "can not create a log: id=" + logId + ", offset=" + offset + ", length=" + length
              + ", checksum="
              + checksum);
    }

    if (length == 0) {
      return createEmptyLog(uuid, logId, offset, checksum, length);
    } else {
      FastBuffer fastBuf = allocateBuffer(length, logPoolType, archiveId, timeout);
      return new MutationLogEntry(uuid, logId, archiveId, offset, fastBuf, checksum);
    }
  }

  public static MutationLogEntry createLog(long uuid, long logId, long offset, byte[] data,
      long checksum, LogPoolType logPoolType) throws NoAvailableBufferException {
    return createLog(uuid, logId, MutationLogEntry.DEFAULT_ARCHIVE_ID, offset, data, checksum,
        logPoolType);
  }

  public static MutationLogEntry createLog(long uuid, long logId, Long archiveId, long offset,
      byte[] data, long checksum,  LogPoolType logPoolType) throws NoAvailableBufferException {
    if (logId == -1 || offset == -1) {
      Validate.isTrue(false,
          "can not create a log: id=" + logId + ", offset=" + offset + ", data=" + data
              + ", checksum="
              + checksum);
    }

    if (data == null || data.length == 0) {
      return createEmptyLog(uuid, logId, offset, checksum, 0);
    } else {
      FastBuffer fastBuf = allocateBuffer(data, logPoolType, archiveId,
          cfg.getWaitForMemoryBufferTimeoutMs());
      return new MutationLogEntry(uuid, logId, archiveId, offset, fastBuf, checksum);
    }
  }

  public static MutationLogEntry createLog(MutationLogEntry oldLog)
      throws NoAvailableBufferException {
    MutationLogEntry newLog = createLog(oldLog.getUuid(), oldLog.getLogId(), oldLog.getArchiveId(),
        oldLog.getOffset(), oldLog.getData(), oldLog.getChecksum(),
        LogPoolType.primaryLogPool);
    newLog.setStatus(oldLog.getStatus());
    if (oldLog.isApplied()) {
      newLog.apply();
    }
    if (oldLog.isPersisted()) {
      newLog.setPersisted();
    }
    return newLog;
  }

  public static MutationLogEntry createLogForPrimary(long uuid, long logId, long offset,
      byte[] data, long checksum)
      throws NoAvailableBufferException {
    return createLog(uuid, logId, MutationLogEntry.DEFAULT_ARCHIVE_ID, offset, data, checksum,
         LogPoolType.primaryLogPool);
  }

  public static MutationLogEntry createLogForPrimary(long uuid, long logId, long archievId,
      long offset, byte[] data, long checksum)
      throws NoAvailableBufferException {
    return createLog(uuid, logId, archievId, offset, data, checksum,
        LogPoolType.primaryLogPool);
  }
  
  public static MutationLogEntry createLogForPrimary(long uuid, long logId, long archiveId,
      long offset, int length, long checksum)
      throws NoAvailableBufferException {
    return createLog(uuid, logId, archiveId, offset, length, checksum,
        LogPoolType.primaryLogPool, cfg.getWaitForMemoryBufferTimeoutMs());
  }

  public static MutationLogEntry createLogForPrimary(long uuid, long logId, long offset,
      int length, long checksum, int timeout)
      throws NoAvailableBufferException {
    return createLog(uuid, logId, MutationLogEntry.DEFAULT_ARCHIVE_ID, offset, length, checksum,
         LogPoolType.primaryLogPool, timeout);
  }

  public static MutationLogEntry createLogForPrimary(long uuid, long logId, long archiveId,
      long offset, int length, long checksum, int timeout)
      throws NoAvailableBufferException {
    return createLog(uuid, logId, archiveId, offset, length, checksum,
        LogPoolType.primaryLogPool, timeout);
  }

  public static MutationLogEntry createLogForSecondary(long uuid, long logId, long archiveId,
      long offset, byte[] data, long checksum) throws NoAvailableBufferException {
   
    return createLog(uuid, logId, archiveId, offset, data, checksum,
        LogPoolType.secondaryLogPool);
  }

  public static MutationLogEntry createLogForSecondary(long uuid, long logId, long archiveId,
      long offset, int length, long checksum)
      throws NoAvailableBufferException {
   
    return createLog(uuid, logId, archiveId, offset, length, checksum,
        LogPoolType.secondaryLogPool, cfg.getWaitForMemoryBufferTimeoutMs());
  }

  public static MutationLogEntry createLogForSecondary(long uuid, long logId, long offset,
      int length, long checksum, int timeout)
      throws NoAvailableBufferException {
   
    return createLog(uuid, logId, MutationLogEntry.DEFAULT_ARCHIVE_ID, offset, length, checksum,
        LogPoolType.secondaryLogPool, timeout);
  }

  public static MutationLogEntry createLogForSecondary(long uuid, long logId, long archiveId,
      long offset, int length,
      long checksum,  int timeout) throws NoAvailableBufferException {
   
    return createLog(uuid, logId, archiveId, offset, length, checksum,
        LogPoolType.secondaryLogPool, timeout);
  }

  public static MutationLogEntry createLogForSyncLog(long uuid, long logId, long offset,
      byte[] data, long checksum)
      throws NoAvailableBufferException {
    try {
     
      return createLog(uuid, logId, MutationLogEntry.DEFAULT_ARCHIVE_ID, offset, data, checksum,
          LogPoolType.syncLogPool);
    } catch (NoAvailableBufferException e) {
      try {
        FastBuffer fastBuf = allocateBuffer(data, LogPoolType.secondaryLogPool, 0,
            cfg.getWaitForMemoryBufferTimeoutMs() / 3);
        return new MutationLogEntry(uuid, logId, MutationLogEntry.DEFAULT_ARCHIVE_ID, offset,
            fastBuf, checksum);
      } catch (NoAvailableBufferException ne) {
       
        try {
         
          HeapMemoryFastBufferImpl fastBufferImpl = membufferList
              .allocate(cfg.getWaitForMemoryBufferTimeoutMs() / 3, TimeUnit.MILLISECONDS);
          if (fastBufferImpl != null) {
            fastBufferImpl.allocate(data);
            return new MutationLogEntry(uuid, logId, MutationLogEntry.DEFAULT_ARCHIVE_ID, offset,
                fastBufferImpl, checksum);
          }
        } catch (Exception e1) {
          logger
              .error("can not create log from free buffer({}) with logID", data.length, logId, e1);
          throw new NoAvailableBufferException(e1);
        }
      }
    }
    throw new NoAvailableBufferException("can not create log:" + logId);
  }

  public static MutationLogEntry createEmptyLog(long uuid, long logId, long offset, long checksum) {
    return createEmptyLog(uuid, logId, offset, checksum, 0);
  }

  public static MutationLogEntry createEmptyLog(long uuid, long logId, long offset, long checksum,
      int length) {
    MutationLogEntry log = new MutationLogEntry(uuid, logId,
        MutationLogEntry.DEFAULT_ARCHIVE_ID, offset, null,
        checksum);
    log.setLength(length);
    return log;
  }

  public static MutationLogEntry createDummyLog() {
    return createEmptyLog(1L, 1L, 0L, 1L, 0);
  }

  public static MutationLogEntry createHeadLog() {
    return createEmptyLog(-1L, -1L, -1L, 0L, 0);
  }

  public static MutationLogEntry createLogFromPersistentStore(long uuid, long id, long offset,
      int dataLength,
      LogStatus logStatus, int snapshotVersion) {
    MutationLogEntry log = createEmptyLog(uuid, id, offset, 0, dataLength);

    if (logStatus == LogStatus.Committed) {
      log.commitAndApply();
    } else {
      log.confirmAbortAndApply();
    }

    log.setPersisted();
    return log;
  }

  public static MutationLogEntry cloneLog(MutationLogEntry srcLog)
      throws NoAvailableBufferException {
    MutationLogEntry logEntry = createLogForPrimary(srcLog.getUuid(), srcLog.getLogId(),
        srcLog.getArchiveId(),
        srcLog.getOffset(), srcLog.getData(), srcLog.getChecksum());
    logEntry.setStatus(srcLog.getStatus());
    return logEntry;
  }

  private static FastBuffer allocateBuffer(int length, LogPoolType logPoolType, long archiveId,
      int waitTimeOut)
      throws NoAvailableBufferException {
   
    FastBuffer dataBuf = null;
    FastBufferManagerProxy fastBufferManagerProxy = null;
    try {
      if (logPoolType == LogPoolType.primaryLogPool) {
        fastBufferManagerProxy = archiveIdToFastBufferManagerMap.get(archiveId);
        if (fastBufferManagerProxy == null) {
          logger.error("fastBufferManagerProxy is null ,archiveId is {}", archiveId);
          throw new NoAvailableBufferException();
        }
        dataBuf = allocateBufferAndCopyData(
            fastBufferManagerProxy, length, waitTimeOut);
      } else if (logPoolType == LogPoolType.secondaryLogPool) {
        fastBufferManagerProxy = archiveIdToSecondFastBufferManagerMap.get(archiveId);
        if (fastBufferManagerProxy == null) {
          throw new NoAvailableBufferException();
        }
        dataBuf = allocateBufferAndCopyData(
            fastBufferManagerProxy, length, waitTimeOut);
      } else if (logPoolType == LogPoolType.syncLogPool) {
        dataBuf = allocateBufferAndCopyData(
            syncLogFastBufferManager, length, waitTimeOut);
      } else {
        logger.error("unknow log pool type: {}", logPoolType);
      }
    } catch (NoAvailableBufferException e) {
      if (fileFastBufferManager != null) {
        dataBuf = allocateBufferAndCopyData(
            fileFastBufferManager, length, cfg.getWaitFileBufferTimeOutMs());
      } else {
        throw new NoAvailableBufferException("Can not allocation fastbuff for size " + length);
      }
    }
    return dataBuf;
  }

  private static FastBuffer allocateBuffer(byte[] src, LogPoolType logPoolType, long archiveId,
      int waitTimeOut)
      throws NoAvailableBufferException {
    FastBuffer dataBuf = null;
    FastBufferManagerProxy fastBufferManagerProxy = null;
    try {
      if (logPoolType == LogPoolType.primaryLogPool) {
        fastBufferManagerProxy = archiveIdToFastBufferManagerMap.get(archiveId);
        if (fastBufferManagerProxy == null) {
          throw new NoAvailableBufferException();
        }

        dataBuf = allocateBufferAndCopyData(
            fastBufferManagerProxy, src, waitTimeOut);
      } else if (logPoolType == LogPoolType.secondaryLogPool) {
        fastBufferManagerProxy = archiveIdToSecondFastBufferManagerMap.get(archiveId);
        if (fastBufferManagerProxy == null) {
          throw new NoAvailableBufferException();
        }
        dataBuf = allocateBufferAndCopyData(
            fastBufferManagerProxy, src, waitTimeOut);
      } else if (logPoolType == LogPoolType.syncLogPool) {
        dataBuf = allocateBufferAndCopyData(
            syncLogFastBufferManager, src, waitTimeOut);
      } else {
        logger.error("unknow log pool type: {}", logPoolType);
      }
    } catch (NoAvailableBufferException e) {
      if (fileFastBufferManager != null) {
        dataBuf = allocateBufferAndCopyData(
            fileFastBufferManager, src, cfg.getWaitFileBufferTimeOutMs());
      } else {
        throw new NoAvailableBufferException("Can not allocation fastbuff for size " + src.length);
      }
    }
    return dataBuf;
  }

  private static FastBuffer allocateBufferAndCopyData(
      FastBufferManagerProxy bufferManager, int length, int waitTimeOut)
      throws NoAvailableBufferException {
   
    FastBuffer dataBuf = bufferManager.allocateBuffer(length, waitTimeOut);
    return dataBuf;
  }

  private static FastBuffer allocateBufferAndCopyData(
      FastBufferManagerProxy bufferManager, byte[] src, int waitTimeOut)
      throws NoAvailableBufferException {
   
    FastBuffer dataBuf = bufferManager.allocateBuffer(src.length, waitTimeOut);
    dataBuf.put(src);
    return dataBuf;
  }

  public static boolean releaseLogData(MutationLogEntry logEntry) {
    if (logEntry != null) {
      return releaseLogData(logEntry.releaseBuffer(), logEntry.getArchiveId());
    } else {
      return false;
    }
  }

  private static boolean releaseLogData(FastBuffer dataBuf, long archivId) {
   
    if (dataBuf == null) {
      return false;
    }

    if (dataBuf instanceof TlsfFileBufferManager.FileFastBufferImpl) {
      fileFastBufferManager.releaseBuffer(dataBuf);
    } else if (dataBuf instanceof HeapMemoryFastBufferImplExtended) {
      membufferList.free((HeapMemoryFastBufferImplExtended) dataBuf);
    } else if (dataBuf instanceof PrimaryFastBufferImpl) {
      FastBufferManagerProxy fastBufferManagerProxy = archiveIdToFastBufferManagerMap.get(archivId);
      if (fastBufferManagerProxy == null) {
        logger.warn("the log archiveId is error");
        return true;
      }
      fastBufferManagerProxy.releaseBuffer(dataBuf);
    } else if (dataBuf instanceof SyncLogFastBufferImpl) {
      syncLogFastBufferManager.releaseBuffer(dataBuf);
    } else if (dataBuf instanceof WtsHeapFastBufferImpl) {
     
      logger.info("release heap memory. data size:{}", dataBuf.size());
      dataBuf = null;
    } else {
      FastBufferManagerProxy fastBufferManagerProxy = archiveIdToFastBufferManagerMap.get(archivId);
      if (fastBufferManagerProxy == null) {
        logger.warn("the log archiveId is error");
        return true;
      }
      fastBufferManagerProxy.releaseBuffer(dataBuf);
    }
    return true;
  }

  public static float getFreeRatioOfFastBuffer(Long archiveId, LogPoolType logPoolType) {
    FastBufferManagerProxy fbManager = logPoolType == LogPoolType.primaryLogPool
        ? archiveIdToFastBufferManagerMap.get(archiveId) :
        logPoolType == LogPoolType.secondaryLogPool 
            ? archiveIdToSecondFastBufferManagerMap.get(archiveId) :
            syncLogFastBufferManager;
    if (fbManager == null) {
      return 0;
    }
    return (float) fbManager.getFreeSectorCount() / (float) fbManager.getTotalSectorCount();
  }

  protected static class HeapMemoryFastBufferManager<T extends HeapIndependentObject> {
    final int capacity;

    final List<T> items;

    private final ReentrantLock lock;

    private final Condition notEmpty;

    public HeapMemoryFastBufferManager(int capacity) {
      if (capacity < 0) {
        throw new IllegalArgumentException();
      }
      this.capacity = capacity;

      items = new LinkedList<T>();
      lock = new ReentrantLock();
      notEmpty = lock.newCondition();
    }

    public HeapMemoryFastBufferManager(Collection<T> c) {
      this(c.size());
      for (T t : c) {
        t.free();
        items.add(t);
      }
    }

    public T allocate() {
      final ReentrantLock lock = this.lock;
      lock.lock();
      try {
       
        if (items.size() > 0) {
          return extract();
        } else {
          return null;
        }
      } finally {
        lock.unlock();
      }
    }

    public T allocate(long timeout, TimeUnit unit) throws InterruptedException {
      long nanos = unit.toNanos(timeout);
      final ReentrantLock lock = this.lock;
      lock.lockInterruptibly();
      try {
        while (items.size() == 0) {
          if (nanos <= 0) {
           
            return null;
          }
          nanos = notEmpty.awaitNanos(nanos);
        }

        return extract();
      } finally {
        lock.unlock();
      }
    }

    public boolean free(T t) {
      if (t == null) {
        return false;
      }

      final ReentrantLock lock = this.lock;
      lock.lock();
      try {
        boolean success = false;
        if (!t.isAllocated()) {
          logger.warn("{} has been freed already.", t);
         
          return success;
        }

        t.free();
        if (items.size() < capacity) {
          items.add(t);
          success = true;
        }
        notEmpty.signal();
        return success;
      } finally {
        lock.unlock();
      }
    }

    private T extract() {
      T t = items.remove(0);
      t.allocate();
      return t;
    }
  }

  private static class HeapMemoryFastBufferImplExtended extends HeapMemoryFastBufferImpl {
    public HeapMemoryFastBufferImplExtended(byte[] buffer) {
      super(buffer);
    }

    @Override
    public FastBuffer allocate(byte[] data) {
      return super.allocate(data);
    }
  }
}
