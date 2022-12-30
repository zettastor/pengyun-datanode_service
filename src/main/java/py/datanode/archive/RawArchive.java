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

package py.datanode.archive;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.CompletionHandler;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.AbstractSegmentUnitMetadata;
import py.archive.Archive;
import py.archive.ArchiveOptions;
import py.archive.ArchiveStatus;
import py.archive.ArchiveStatusListener;
import py.archive.ArchiveType;
import py.archive.RawArchiveMetadata;
import py.archive.brick.BrickMetadata;
import py.archive.page.PageAddress;
import py.archive.page.PageAddressImpl;
import py.archive.segment.SegId;
import py.archive.segment.SegmentUnitMetadata;
import py.archive.segment.SegmentUnitStatus;
import py.archive.segment.SegmentUnitType;
import py.archive.segment.recurring.MigratingSegmentUnitMetadata;
import py.archive.segment.recurring.SegmentUnitTaskCallback;
import py.archive.segment.recurring.SegmentUnitTaskExecutor;
import py.common.VolumeMetadataJsonParser;
import py.common.cache.CachedProducer;
import py.common.tlsf.bytebuffer.manager.TlsfByteBufferManager;
import py.common.tlsf.bytebuffer.manager.TlsfByteBufferManagerFactory;
import py.datanode.configuration.LogPersistingConfiguration;
import py.datanode.exception.CannotAllocMoreArbiterException;
import py.datanode.exception.CannotAllocMoreFlexibleException;
import py.datanode.exception.InvalidSegmentStatusException;
import py.datanode.exception.LogIdNotFoundException;
import py.datanode.exception.NoNeedToBeDeletingException;
import py.datanode.exception.StaleMembershipException;
import py.datanode.exception.StorageBrokenException;
import py.datanode.segment.PersistDataContext;
import py.datanode.segment.PersistDataContextFactory;
import py.datanode.segment.PersistDataToDiskEngineImpl;
import py.datanode.segment.PersistDataType;
import py.datanode.segment.SegmentUnit;
import py.datanode.segment.SegmentUnitFactory;
import py.datanode.segment.datalog.LogIdGenerator;
import py.datanode.segment.datalog.MutationLogEntry;
import py.datanode.segment.datalog.MutationLogEntry.LogStatus;
import py.datanode.segment.datalog.SegmentLogMetadata;
import py.datanode.segment.datalog.persist.LogStorageReader;
import py.datanode.segment.datalog.sync.log.SyncLogTaskExecutor;
import py.datanode.segment.datalog.sync.log.driver.SecondaryPclDriver;
import py.datanode.segment.datalogbak.catchup.CatchupLogContextKey;
import py.datanode.segment.datalogbak.catchup.CatchupLogContextKey.CatchupLogDriverType;
import py.datanode.segment.heartbeat.HostHeartbeat;
import py.exception.ArchiveNotExistException;
import py.exception.ArchiveStatusException;
import py.exception.NotEnoughSpaceException;
import py.exception.SegmentUnitRecoverFromDeletingFailException;
import py.exception.StorageException;
import py.instance.Group;
import py.instance.InstanceId;
import py.membership.SegmentMembership;
import py.storage.Storage;
import py.third.rocksdb.KvStoreException;
import py.volume.VolumeId;
import py.volume.VolumeType;

public class RawArchive extends Archive {
  private static final Logger logger = LoggerFactory.getLogger(RawArchive.class);

  private static long quarantineZoneMSToRecoverDeletingSegment;
  private static long msFromDeletingToDeletedForSegment;
  private final RawArchiveMetadata archiveMetadata;
  private final ReentrantLock spaceLock;
  private String deviceName;
  private Map<SegId, SegmentUnit> segUnits;
  private ArchiveSpaceManager spaceManager;

  private long physicalSegmentUnitSize;
  private SegmentUnitTaskCallback callback;
  private SegmentUnitTaskExecutor catchupLogEngine;
  private SegmentUnitTaskExecutor stateProcessingEngine;
  private HostHeartbeat hostHeartbeat;
  private Map<VolumeId, Set<SegId>> volumeToSegmentRelationship;
  private LogStorageReader logStorageReader;
  private AtomicBoolean notEnoughSpaceForSnapshot = new AtomicBoolean(false);
  private CachedProducer<PageAddress> shadowPageProducer;
  private SyncLogTaskExecutor syncLogTaskExecutor;
  private volatile long lastUserIoTime = 0;

  protected RawArchive(RawArchiveMetadata archiveMetadata,
      List<SegmentUnitMetadata> srcSegUnitMetadatas, List<BrickMetadata> brickMetadataList,
      Storage storage, SegmentUnitTaskCallback callback, boolean isArbiter,
      SyncLogTaskExecutor syncLogTaskExecutor) {
    super(storage);
    this.archiveMetadata = archiveMetadata;
    this.segUnits = new ConcurrentHashMap<>();
    this.spaceManager = new ArchiveSpaceManagerV2Impl(this, srcSegUnitMetadatas,
        brickMetadataList);
    this.syncLogTaskExecutor = syncLogTaskExecutor;
    this.volumeToSegmentRelationship = new HashMap<>();
    for (SegmentUnitMetadata metadata : srcSegUnitMetadatas) {
      if (!metadata.getStatus().equals(SegmentUnitStatus.Deleted)) {
        SegmentUnit segmentUnit = SegmentUnitFactory.build(metadata, this);
        if (syncLogTaskExecutor != null) {
          SecondaryPclDriver pclDriver = new SecondaryPclDriver(segmentUnit,
              syncLogTaskExecutor.getConfiguration(), syncLogTaskExecutor);
          segmentUnit.setSecondaryPclDriver(pclDriver);
        }

        segmentUnit.getSegmentUnitMetadata().setDiskName(archiveMetadata.getDeviceName());
        segmentUnit.getSegmentUnitMetadata().setArchiveId(archiveMetadata.getArchiveId());
        segUnits.put(metadata.getSegId(), segmentUnit);

        SegmentUnit preSegUnit = segUnits.putIfAbsent(metadata.getSegId(), segmentUnit);
        if (preSegUnit != null) {
          logger.warn("previous segment unit:{} current segment unit:{}, segId {}",
              preSegUnit.getArchive(), segmentUnit.getArchive(), metadata.getSegId());

          if (segmentUnit.getArchive().getArchiveStatus() == ArchiveStatus.GOOD) {
            segUnits.put(metadata.getSegId(), segmentUnit);
          } else {
            continue;
          }
        }
        Set<SegId> segIds = volumeToSegmentRelationship.get(metadata.getSegId().getVolumeId());
        if (segIds != null) {
          segIds.add(metadata.getSegId());
        } else {
          segIds = new HashSet<>();
          segIds.add(metadata.getSegId());
          volumeToSegmentRelationship.put(metadata.getSegId().getVolumeId(), segIds);
        }
      }
    }

    this.physicalSegmentUnitSize = calculatePhysicalSegmentUnitSize(
        archiveMetadata.getSegmentUnitSize(),
        archiveMetadata.getPageSize());
    this.callback = callback;
    this.spaceLock = new ReentrantLock(true);

    this.updateLogicalFreeSpaceToMetaData();
  }

  protected RawArchive(RawArchiveMetadata archiveMetadata,
      List<SegmentUnitMetadata> srcSegUnitMetadatas, List<BrickMetadata> brickMetadataList,
      Storage storage, SegmentUnitTaskCallback callback, SyncLogTaskExecutor syncLogTaskExecutor) {
    this(archiveMetadata, srcSegUnitMetadatas, brickMetadataList, storage, callback, false,
        syncLogTaskExecutor);
  }

  private static long calculateSpacesForPageMetadata(long segmentUnitSize, int pageSize) {
    int numPages = (int) (segmentUnitSize / pageSize);
    Validate.isTrue(segmentUnitSize % pageSize == 0,
        "segmentUnitSize %s must be multiple times of page size %s",
        segmentUnitSize, pageSize);

    Validate.isTrue(numPages >= 0);

    return numPages * (ArchiveOptions.PAGE_PHYSICAL_SIZE - pageSize);
  }

  // return the physical segment unit size
  private static long calculatePhysicalSegmentUnitSize(long logicSegmentUnitSize, int pageSize) {
    long spaceForAllPageMetadataInSegmentUnit = calculateSpacesForPageMetadata(logicSegmentUnitSize,
        pageSize);
    return logicSegmentUnitSize + spaceForAllPageMetadataInSegmentUnit;
  }

  public static void setQuarantineZoneMsToRecoverDeletingSegment(
      long quarantineZoneMsToRecoverDeletingSegment) {
    RawArchive.quarantineZoneMSToRecoverDeletingSegment = quarantineZoneMsToRecoverDeletingSegment;
  }

  public static void setMsFromDeletingToDeletedForSegment(long msFromDeletingToDeletedForSegment) {
    RawArchive.msFromDeletingToDeletedForSegment = msFromDeletingToDeletedForSegment;
  }

  public boolean preAllocateBrick(int lowerThreshold, int upperThreshold,
      boolean isArbitPreAllocator) {
    if (this.getArchiveStatus().equals(ArchiveStatus.GOOD) || this.getArchiveStatus()
        .equals(ArchiveStatus.DEGRADED)) {
      spaceLock.lock();
      try {
        spaceManager.preAllocateBrick(lowerThreshold, upperThreshold, isArbitPreAllocator);
      } finally {
        spaceLock.unlock();
      }

      return true;
    }

    return false;
  }

  public void setSyncLogTaskExecutor(
      SyncLogTaskExecutor syncLogTaskExecutor) {
    this.syncLogTaskExecutor = syncLogTaskExecutor;
  }

  public void setCallback(SegmentUnitTaskCallback callback) {
    this.callback = callback;
  }

  public SegmentUnit addSegmentUnit(SegId segId, SegmentMembership membership,
      VolumeType volumeType,
      String volumeMetadataJson, String accountMetadataJson,
      SegmentUnitType segmentUnitType, long srcVolumeId,
      SegmentMembership srcMembership, boolean isSecondaryCandidate,
      InstanceId replacee)
      throws StorageBrokenException, CannotAllocMoreArbiterException, NotEnoughSpaceException,
      StorageException, IOException, CannotAllocMoreFlexibleException {
    Validate.isTrue(canBeUsed());
    if (getStorage().isClosed()) {
      throw new StorageBrokenException();
    }

    if (null != segUnits.get(segId)) {
      throw new IllegalArgumentException("SegId " + segId + " alreadyExists ");
    }

    this.spaceLock.lock();
    try {
      SegmentUnitMetadata newMetadata = this.spaceManager
          .allocateSegmentUnit(segId, membership, volumeType,
              volumeMetadataJson,
              accountMetadataJson, segmentUnitType, srcVolumeId, srcMembership,
              isSecondaryCandidate, replacee);
      newMetadata.setDiskName(archiveMetadata.getDeviceName());
      newMetadata.setArchiveId(archiveMetadata.getArchiveId());

      SegmentUnit newSegmentUnit = SegmentUnitFactory.build(newMetadata, this);
      SecondaryPclDriver pclDriver = new SecondaryPclDriver(newSegmentUnit,
          syncLogTaskExecutor.getConfiguration(), syncLogTaskExecutor);
      newSegmentUnit.setSecondaryPclDriver(pclDriver);

      segUnits.put(segId, newSegmentUnit);

      Set<SegId> segIds = volumeToSegmentRelationship.get(newSegmentUnit.getSegId().getVolumeId());
      if (segIds != null) {
        segIds.add(newSegmentUnit.getSegId());
      } else {
        segIds = new HashSet<SegId>();
        segIds.add(newSegmentUnit.getSegId());
        volumeToSegmentRelationship.put(newSegmentUnit.getSegId().getVolumeId(), segIds);
      }

      updateLogicalFreeSpaceToMetaData();

      return newSegmentUnit;
    } finally {
      this.spaceLock.unlock();
    }
  }

  public SegmentUnit addSegmentUnit(SegId segId, SegmentMembership membership,
      SegmentUnitType segmentUnitType)
      throws StorageBrokenException, CannotAllocMoreArbiterException, NotEnoughSpaceException,
      StorageException, IOException, CannotAllocMoreFlexibleException {
    SegmentUnit segmentUnit = addSegmentUnit(segId, membership, VolumeType.REGULAR,
        null, null,
        segmentUnitType, 0L, null, false, null);
    return segmentUnit;
  }

  public synchronized void migrateSegmentUnitMetadataToThisArchive(
      SegmentUnitMetadata segmentUnitMetadata,
      MigratingSegmentUnitMetadata migratingSegmentUnitMetadata) {
    this.spaceLock.lock();
    try {
      segmentUnitMetadata.setStorage(this.getStorage());
      spaceManager
          .replaceAbstractSegmentUnitMetadata(migratingSegmentUnitMetadata, segmentUnitMetadata);
    } finally {
      this.spaceLock.unlock();
    }
  }

  public synchronized void freeSegmentUnitMetadata(SegmentUnitMetadata segmentUnitMetadata) {
    this.spaceLock.lock();
    try {
      segUnits.remove(segmentUnitMetadata.getSegId());
      spaceManager.freeAbstractSegmentUnitMetadata(segmentUnitMetadata);
    } finally {
      this.spaceLock.unlock();
    }
  }

  public void persistSegmentUnitMetaAndBitmap(SegmentUnitMetadata newMetadata, boolean syncPersist)
      throws Exception {
    PersistDataContext context;
    segUnits.get(newMetadata.getSegId()).lockStatus();
    try {
      SegmentUnitMetadata segmentUnitMetadata = new SegmentUnitMetadata(newMetadata) {
        @Override
        public void setIsPersistDeletedStatus(boolean status) {
          newMetadata.setIsPersistDeletedStatus(status);
        }
      };
      context = PersistDataContextFactory
          .generatePersistDataContext(segmentUnitMetadata,
              PersistDataType.SegmentUnitMetadataAndBitMap, this);
    } finally {
      segUnits.get(newMetadata.getSegId()).unlockStatus();
    }
    if (syncPersist) {
      PersistDataToDiskEngineImpl.getInstance().syncPersistData(context);
    } else {
      PersistDataToDiskEngineImpl.getInstance().asyncPersistData(context);
    }
  }

  public void persistSegmentUnitMetaAndBitmap(SegmentUnitMetadata newMetadata) throws Exception {
    persistSegmentUnitMetaAndBitmap(newMetadata, true);
  }

  public void asyncPersistSegmentUnitMetaAndBitmap(SegmentUnitMetadata newMetadata)
      throws Exception {
    persistSegmentUnitMetaAndBitmap(newMetadata, false);
  }

  public void persistSegmentUnitMeta(SegmentUnitMetadata newMetadata, boolean syncPersist) {
    PersistDataContext context = PersistDataContextFactory
        .generatePersistDataContext(newMetadata, PersistDataType.SegmentUnitMetadata, this);
    if (syncPersist) {
      PersistDataToDiskEngineImpl.getInstance().syncPersistData(context);
    } else {
      PersistDataToDiskEngineImpl.getInstance().asyncPersistData(context);
    }
  }

  public void persistSegmentUnitMetaAndBitmapAndCleanSnapshot(SegmentUnit segmentUnit,
      CompletionHandler<Integer, SegmentUnitMetadata> callback) throws Exception {
    logger.debug("the data is {} ", segmentUnit);
    try {
      PersistDataToDiskEngineImpl.getInstance()
          .writeSegmentUnitMetadataAndBitMapToDisk(segmentUnit, callback);
    } catch (Exception e) {
      logger.error("failed to persist data:{} to disk", segmentUnit, e);
      callback.failed(e, segmentUnit.getSegmentUnitMetadata());
    }
    return;
  }

  public void persistSegmentUnitMetaAndBitmapAndCleanSnapshotAndBrickMetadata(
      SegmentUnit segmentUnit,
      CompletionHandler<Integer, SegmentUnitMetadata> callback) throws Exception {
    logger.debug("the data is {} ", segmentUnit);
    try {
      PersistDataToDiskEngineImpl.getInstance()
          .writeSegmentUnitMetadataAndBitMapAndBrickMetadataToDisk(segmentUnit, callback);
    } catch (Exception e) {
      logger.error("failed to persist data:{} to disk", segmentUnit, e);
      callback.failed(e, segmentUnit.getSegmentUnitMetadata());
    }
    return;
  }

  public void persistSegmentUnitMetaAndBitmapAndCleanSnapshotAndBrickMetadata(
      SegmentUnitMetadata segmentUnitMetadata) throws Exception {
    logger.debug("the data is {} ", segmentUnitMetadata);
    try {
      PersistDataToDiskEngineImpl.getInstance()
          .writeBrickMetadata(segmentUnitMetadata.getBrickMetadata());
      PersistDataToDiskEngineImpl.getInstance()
          .writeSegmentUnitMetadataAndBitMapToDisk(segmentUnitMetadata);
    } catch (Exception e) {
      logger.error("failed to persist data:{} to disk", segmentUnitMetadata, e);
    }
    return;
  }

  public void persisBrickMetaAndBitmap(BrickMetadata brickMetadata)
      throws StorageException, JsonProcessingException {
    logger.debug("the data is {} ", brickMetadata);
    PersistDataToDiskEngineImpl.getInstance()
        .writeBrickMetadata(brickMetadata);
    return;
  }

  public void persisBrickMetaAndBitmap(BrickMetadata brickMetadata,
      CompletionHandler<Integer, BrickMetadata> callback) {
    logger.debug("the data is {} ", brickMetadata);
    try {
      PersistDataToDiskEngineImpl.getInstance()
          .writeBrickMetadata(brickMetadata, callback);
    } catch (Exception e) {
      logger.error("failed to persist data:{} to disk", brickMetadata, e);
      callback.failed(e, brickMetadata);
    }
    return;
  }

  public List<BrickMetadata> getShadowUnitList() {
    return this.spaceManager.getShadowUnitList();
  }

  public Storage getStorage() {
    return storage;
  }

  /**
   * Replace the storage when disk plugged in again This function is called when archive is plugged
   * in real time.
   */
  public void setStorage(Storage newStorage) {
    super.setStorage(newStorage);

    for (Entry<SegId, SegmentUnit> entry : segUnits.entrySet()) {
      entry.getValue().getSegmentUnitMetadata().setStorage(newStorage);
    }
  }

  @Override
  public void addListener(ArchiveStatusListener listener) {
    boolean foundListener = false;
    for (ArchiveStatusListener listenerExist : archiveStatusListers) {
      if (listenerExist == listener) {
        foundListener = true;
      }
    }
    if (!foundListener) {
      this.archiveStatusListers.add(listener);
    }
  }

  @Override
  public void clearArchiveStatusListener() {
    this.archiveStatusListers.clear();
  }

  public long getSegmentUnitDataStartPosition() {
    return this.archiveMetadata.getSegmentUnitDataOffset();
  }

  public long getSegmentUnitLogicalDataStartPosition() {
    return this.archiveMetadata.getVirtualSegmentUnitDataOffset();
  }

  public long getLogicalFreeSpace() {
    if (!canBeUsed()) {
      return 0L;
    }
    return this.archiveMetadata.getLogicalFreeSpace();
  }

  public boolean acceptNewSegmentUnitCreation(SegmentUnitType segmentUnitType) {
    if (this.getArchiveStatus() != ArchiveStatus.GOOD) {
      return false;
    }
    spaceLock.lock();
    try {
      if (segmentUnitType == SegmentUnitType.Normal) {
        return spaceManager.acceptNewSegmentUnitCreation()
            && spaceManager.getSegmentUnitInUsedCount()
            < ArchiveOptions.MAX_FLEXIBLE_COUNT;
      } else if (segmentUnitType == SegmentUnitType.Arbiter) {
        return true;
      } else if (segmentUnitType == SegmentUnitType.Flexible) {
        return spaceManager.getSegmentUnitInUsedCount() < ArchiveOptions.MAX_FLEXIBLE_COUNT;
      } else {
        throw new IllegalArgumentException();
      }
    } finally {
      spaceLock.unlock();
    }

  }

  // set flexible segment unit count that can be used.
  private void updateFlexibleSpaceToMetaData() {
    int flexibleInUsedCount = spaceManager.getFlexibleInUsedCount();
    logger.debug("archive flexible in used count {}", flexibleInUsedCount);
    this.archiveMetadata
        .setFreeFlexibleSegmentUnitCount(ArchiveOptions.MAX_FLEXIBLE_COUNT - flexibleInUsedCount);
  }

  public void updateLogicalFreeSpaceToMetaData() {
    if (!canBeUsed()) {
      this.archiveMetadata.setLogicalFreeSpace(0L);
      this.archiveMetadata.setFreeFlexibleSegmentUnitCount(0);
      return;
    }

    spaceLock.lock();
    try {
      long logicalFreeSpace = spaceManager.getFreeBrickSpace();
      logger.debug("archive logical free space {}", logicalFreeSpace);
      this.archiveMetadata.setLogicalFreeSpace(logicalFreeSpace);
      updateFlexibleSpaceToMetaData();
    } finally {
      spaceLock.unlock();
    }
  }

  public long getLargestContiguousLogicalFreeSpace() {
    if (!canBeUsed()) {
      return 0L;
    }

    this.spaceLock.lock();
    try {
      return spaceManager.getLargestContiguousFreeSpaceLogically();
    } finally {
      this.spaceLock.unlock();
    }
  }

  public void updateLogicalUsedSpaceToMetaData() {
    if (!canBeUsed()) {
      this.archiveMetadata.setUsedSpace(0L);
      return;
    }

    spaceLock.lock();
    try {
      this.archiveMetadata.setUsedSpace(spaceManager.getUsedSpace());
    } finally {
      spaceLock.unlock();
    }
  }

  public boolean updateSegmentUnitMetadata(SegId segId, String volumeMetadataJson)
      throws InvalidSegmentStatusException, StaleMembershipException {
    return updateSegmentUnitMetadata(segId, null, null, false, volumeMetadataJson, false, true);
  }

  public boolean updateSegmentUnitMetadata(SegId segId, SegmentMembership membership,
      SegmentUnitStatus newStatus, boolean persistAnyway)
      throws InvalidSegmentStatusException, StaleMembershipException {
    return updateSegmentUnitMetadata(segId, membership, newStatus, false, null, persistAnyway,
        true);
  }

  protected boolean updateSegmentUnitMetadata(SegId segId, SegmentMembership membership,
      SegmentUnitStatus newStatus,
      boolean acceptStaleMembership, String volumeMetadataJson, boolean persistAnyway,
      boolean syncPersist)
      throws InvalidSegmentStatusException, StaleMembershipException {
    boolean needToFlushSegmentStatusToDisk = false;
    SegmentUnit segmentUnit = null;

    segmentUnit = segUnits.get(segId);
    if (segmentUnit == null) {
      throw new IllegalArgumentException("cannot find segment :" + segId);
    }
    BrickMetadata brickMetadata = segmentUnit.getSegmentUnitMetadata().getBrickMetadata();
    segmentUnit.lockStatus();
    SegmentUnitStatus oldStatus;
    try {
      logger.debug(
          "updateSegmentUnitMetadata segUnit {}, newStatus {}, membership {},"
              + " volumeMetadataJson {} ",
          segmentUnit, newStatus, membership, volumeMetadataJson);
      oldStatus = segmentUnit.getSegmentUnitMetadata().getStatus();
      SegmentMembership oldMembership = segmentUnit.getSegmentUnitMetadata().getMembership();
      if (internalUpdateSegmentUnitMetadata(segmentUnit, membership, newStatus,
          acceptStaleMembership,
          volumeMetadataJson) && isNeedUpdateSegmentUnitMatadataToDisk(oldMembership, membership,
          oldStatus,
          newStatus, volumeMetadataJson)) {
        needToFlushSegmentStatusToDisk = true;
      }

      boolean oldStatusIsDeleting =
          oldStatus != null && (oldStatus.equals(SegmentUnitStatus.Deleting) || oldStatus
              .equals(SegmentUnitStatus.Broken));
      boolean newStatusIsDeleting =
          newStatus == null ? oldStatusIsDeleting
              : (newStatus.equals(SegmentUnitStatus.Deleting) || newStatus
                  .equals(SegmentUnitStatus.Broken));
      dealWithSpecialStatus(segId, newStatus);
    } finally {
      segmentUnit.unlockStatus();
    }

    if (!persistAnyway && !needToFlushSegmentStatusToDisk) {
      return false;
    }

    ArchiveStatus archiveStatus = getArchiveStatus();
    if (archiveStatus == ArchiveStatus.GOOD || archiveStatus == ArchiveStatus.DEGRADED
        || archiveStatus == ArchiveStatus.OFFLINING) {
      if (newStatus != SegmentUnitStatus.OFFLINED) {
        if (newStatus == SegmentUnitStatus.Deleted && null != brickMetadata) {
          try {
            PersistDataToDiskEngineImpl.getInstance().writeBrickMetadata(brickMetadata);
          } catch (Exception e) {
            logger.error("failed to persist data:{} to disk", brickMetadata, e);
          }
        }
        persistSegmentUnitMeta(segmentUnit.getSegmentUnitMetadata(), syncPersist);

        logger.debug("segId:{} old status:{} new status:{}", segId, oldStatus, newStatus);
        return true;
      }
    }

    return false;
  }

  public boolean updateSegmentUnitMetadata(SegId segId, SegmentMembership membership,
      SegmentUnitStatus newStatus) throws InvalidSegmentStatusException, StaleMembershipException {
    return updateSegmentUnitMetadata(segId, membership, newStatus, false, null, false, true);
  }

  public boolean asyncUpdateSegmentUnitMetadata(SegId segId, SegmentMembership membership,
      SegmentUnitStatus newStatus) throws InvalidSegmentStatusException, StaleMembershipException {
    return updateSegmentUnitMetadata(segId, membership, newStatus, false, null, false, false);
  }

  public boolean asyncUpdateSegmentUnitMetadata(SegId segId, SegmentMembership membership)
      throws Exception {
    return updateSegmentUnitMetadata(segId, membership, null, false, null, false, false);
  }

  private boolean isNeedUpdateSegmentUnitMatadataToDisk(SegmentMembership oldMembership,
      SegmentMembership newMembership, SegmentUnitStatus oldStatus, SegmentUnitStatus newStatus,
      String volumeMetadataJson) {
    if (newStatus == SegmentUnitStatus.Broken) {
      return false;
    }

    if (newMembership != null && !oldMembership.equals(newMembership)) {
      return true;
    }

    if (volumeMetadataJson != null) {
      return true;
    }

    if (newStatus != null) {
      if (oldStatus != newStatus) {
        if (newStatus.isFinalStatus()) {
          return true;
        }

        if (newStatus == SegmentUnitStatus.Start && oldStatus.isFinalStatus()) {
          return true;
        }
      }
    }

    return false;
  }

  private boolean internalUpdateSegmentUnitMetadata(SegmentUnit unit,
      SegmentMembership newMembership,
      SegmentUnitStatus newStatus, boolean acceptStaleMembership, String volumeMetadataJson)
      throws StaleMembershipException, InvalidSegmentStatusException {
    SegmentUnitMetadata metadata = unit.getSegmentUnitMetadata();
    SegmentUnitStatus currentStatus = metadata.getStatus();
    SegId segId = unit.getSegId();

    logger.warn("internalUpdateSegmentUnitMetadata, segUnit is {}, new status {}, newMembership {}",
        unit,
        newStatus, newMembership);

    if (currentStatus == SegmentUnitStatus.Deleted) {
      throw new IllegalArgumentException(
          "segment:" + segId + " was deleted, and can't update its metadata");
    } else if (currentStatus == SegmentUnitStatus.Deleting) {
      if (newMembership != null && newStatus != SegmentUnitStatus.Start) {
        throw new IllegalArgumentException(
            "can't update segment " + segId + " membership because its status is Deleting");
      }

      if (newStatus != null && !(newStatus == SegmentUnitStatus.Start
          || newStatus == SegmentUnitStatus.Deleted || newStatus == SegmentUnitStatus.Broken)) {
        throw new IllegalArgumentException(
            "Segment:" + segId + " has been marked as deleting and can't update its metadata");
      }
    } else if (currentStatus == SegmentUnitStatus.Broken) {
      if (newStatus != null && newStatus != SegmentUnitStatus.Deleted && (newStatus
          != SegmentUnitStatus.Start)) {
        throw new IllegalArgumentException(
            "can't update segment " + segId + " status from Broken to " + newStatus);
      }

      if (newStatus == SegmentUnitStatus.Start && !getArchiveStatus().canDoIo()) {
        throw new IllegalArgumentException(
            "can't update segment " + segId + " status from Broken to " + newStatus
                + "archive Status" + getArchiveStatus());
      }
    } else if (currentStatus == SegmentUnitStatus.OFFLINING) {
      if (newStatus != null && newStatus != SegmentUnitStatus.OFFLINED
          && newStatus != SegmentUnitStatus.Deleted
          && newStatus != SegmentUnitStatus.Broken && newStatus != SegmentUnitStatus.Start) {
        throw new IllegalArgumentException(
            "can't update segment " + segId + " status from Offlining to " + newStatus);
      }

      if (newMembership != null) {
        throw new IllegalArgumentException(
            "can't update segment " + segId + " membership because its status is offling");
      }
    } else if (currentStatus == SegmentUnitStatus.OFFLINED) {
      if (newStatus != null && newStatus != SegmentUnitStatus.Deleting
          && newStatus != SegmentUnitStatus.Start
          && newStatus != SegmentUnitStatus.Deleted && newStatus != SegmentUnitStatus.Broken) {
        throw new IllegalArgumentException(
            "can't update segment " + segId + " status from OFFLINED to " + newStatus);
      }

      if (newMembership != null) {
        throw new IllegalArgumentException(
            "can't update segment " + segId + " membership because its status is offlined");
      }
    } else if (currentStatus == SegmentUnitStatus.Start) {
      if (newStatus != null && newStatus.isStable()) {
        throw new IllegalArgumentException(
            "can't update segment " + segId + newStatus + "because its status is start");
      }
    }

    boolean updated = false;
    if (newMembership != null) {
      SegmentMembership currentMembership = metadata.getMembership();
      int compareWithNew = currentMembership.compareTo(newMembership);
      int compareEpochWithNew = currentMembership.compareEpoch(newMembership);
      if (acceptStaleMembership || compareWithNew < 0) {
        InstanceId myself = archiveMetadata.getInstanceId();
        if (newMembership.size() < currentMembership.size()) {
          logger.error("{} reducing members' count in membership new one : {}, old one : {}", segId,
              newMembership, currentMembership, new Exception());
        }
        if (compareEpochWithNew < 0) {
          logger.warn("updating to a new epoch ! {} {} {}", segId, currentMembership,
              newMembership);
          unit.getAcceptor().init();
          if (currentStatus == SegmentUnitStatus.Start) {
            unit.getAcceptor().open();
          }
          if (unit.isSecondaryZombie()
              && currentStatus != SegmentUnitStatus.PrePrimary) {
            logger
                .warn("the new membership has a new primary, clear my zoobie status {}",
                    segId);
            unit.clearSecondaryZombie();
          }
          if (newMembership.isPrimary(myself)) {
            unit.getSegmentLogMetadata()
                .updateLogIdGenerator(
                    new LogIdGenerator(newMembership.getSegmentVersion()));
          }
        }

        if (metadata.getStatus() == SegmentUnitStatus.Primary
            || currentMembership.isPrimary(myself)) {
          updateMembershipForHeartBeat(unit, currentMembership, newMembership);
        }

        metadata.setMembership(newMembership);
        updated = true;

      } else if (compareWithNew > 0) {
        throw new StaleMembershipException(newMembership, currentMembership);
      }

    }

    if (newStatus != null && newStatus != currentStatus
        && currentStatus != SegmentUnitStatus.Deleted) {
      updated = true;

      if (newStatus == SegmentUnitStatus.Broken) {
        if (currentStatus.isPrimary() && hostHeartbeat != null) {
          hostHeartbeat.removeRelationBetweenPrimaryAndAllAliveSecondaries(unit);
        }

        metadata.setStatus(newStatus);

        unit.getAcceptor().freeze();
      } else if (newStatus.higher(currentStatus)

          || (newStatus == SegmentUnitStatus.Start && currentStatus != SegmentUnitStatus.Deleted)
          || (
          newStatus == SegmentUnitStatus.Deleted && currentStatus == SegmentUnitStatus.Broken) || (
          newStatus == SegmentUnitStatus.Primary)) {
        metadata.setStatus(newStatus);

        if (newStatus.isPrimary() && !currentStatus.isPrimary()) {
          logger.warn("{} new status {}, current status {}", segId, newStatus, currentStatus);
          if (hostHeartbeat != null) {
            hostHeartbeat.addRelationBetweenPrimaryAndAllAliveSecondaries(unit);
          }
        } else if (currentStatus.isPrimary() && !newStatus.isPrimary()) {
          logger.warn("{} new status {}, current status {}", segId, newStatus, currentStatus);
          if (hostHeartbeat != null) {
            hostHeartbeat.removeRelationBetweenPrimaryAndAllAliveSecondaries(unit);
          }
        }

        if (newStatus == SegmentUnitStatus.Secondary) {
          if (unit.getSegmentLogMetadata() != null) {
            unit.getSegmentLogMetadata().updateLogIdGenerator(null);
          }

          if (unit.getSecondaryCopyPageManager() != null) {
            unit.setSecondaryCopyPageManager(null);
          }
        }

        if (newStatus == SegmentUnitStatus.Start) {
          logger.debug("the new status is Start, we are resetting everything");
          unit.getAcceptor().open();

          unit.clearMyLease();
          unit.clearPeerLeases();
          unit.clearExpiredMembers();

          unit.setDisableExtendPeerLease(false);

          if (unit.getSegmentLogMetadata() != null) {
            unit.getSegmentLogMetadata().clearPeerClIds();
            unit.getSegmentLogMetadata().clearPeerPlIds();
          }

          if (currentStatus != SegmentUnitStatus.PrePrimary) {
            unit.resetPotentialPrimaryId();
            unit.resetPreprimaryDrivingSessionId();
          }

          unit.resetPreprimaryDrivingSessionId();
          unit.setParticipateVotingProcess(true);
          unit.setCopyPageFinished(false);

          unit.getPrimaryDecisionMade().set(false);
        } else if (newStatus != SegmentUnitStatus.ModeratorSelected) {
          unit.getAcceptor().freeze();
        }
      } else {
        throw new InvalidSegmentStatusException(newStatus, currentStatus);
      }
    }

    if (volumeMetadataJson != null) {
      VolumeMetadataJsonParser parserForCurrentVm = new VolumeMetadataJsonParser(
          metadata.getVolumeMetadataJson());
      VolumeMetadataJsonParser parserForReceivedVm = new VolumeMetadataJsonParser(
          volumeMetadataJson);
      if (parserForReceivedVm.getVersion() > parserForCurrentVm.getVersion()) {
        metadata.setVolumeMetadataJson(volumeMetadataJson);
        updated = true;
      }
    }

    if (updated) {
      metadata.setLastUpdated(System.currentTimeMillis());
    }
    return updated;
  }

  private void updateMembershipForHeartBeat(SegmentUnit unit, SegmentMembership oldMembership,
      SegmentMembership newMembership) {
    if (this.hostHeartbeat == null) {
      logger.debug(
          "host heartbeat is null, "
              + "This happen when archive initialize before data node service start");
      return;
    }

    InstanceId myself = archiveMetadata.getInstanceId();
    if (!newMembership.isPrimary(myself)) {
      logger.info("i am no longer primary in membership");
      if (oldMembership.isPrimaryCandidate(newMembership.getPrimary())) {
        logger.info("remove heartbeat members in old membership");
        hostHeartbeat.removeRelationBetweenPrimaryAndAllAliveSecondaries(unit);
      }
      return;
    }

    for (InstanceId oldAliveInstance : oldMembership.getHeartBeatMembers()) {
      if (!newMembership.getHeartBeatMembers().contains(oldAliveInstance)) {
        hostHeartbeat.removeRelationBetweenPrimaryAndAliveSecondary(unit, oldAliveInstance.getId());
      }
    }

    for (InstanceId newInstance : newMembership.getHeartBeatMembers()) {
      if (!oldMembership.getHeartBeatMembers().contains(newInstance)) {
        hostHeartbeat.addRelationBetweenPrimaryAndAliveSecondary(unit, newInstance.getId());
      }
    }

  }

  public Collection<SegmentUnit> getSegmentUnits() {
    return new ArrayList<SegmentUnit>(segUnits.values());
  }

  public int getArbiterNumber() {
    return spaceManager.getArbiterInUsedCount();
  }

  public int getFlexibleNumber() {
    return spaceManager.getFlexibleInUsedCount();
  }

  public synchronized void markSegmentUnitDeleting(SegId segId, boolean syncPersist)
      throws NoNeedToBeDeletingException, Exception {
    logger.debug("mark segment unit deleting: {}, my instance {}", segId,
        archiveMetadata.getInstanceId());
    SegmentUnit segmentUnit = segUnits.get(segId);
    if (segmentUnit == null) {
      throw new IllegalArgumentException("cannot find segment :" + segId);
    }
    segmentUnit.lockStatus();
    boolean persistForce = false;
    try {
      if (segmentUnit.getSegmentUnitMetadata().getStatus() == SegmentUnitStatus.Deleting) {
        logger.warn("segment unit is already in deleting status {}", segId);
        return;
      }
      if (segmentUnit.getSegmentUnitMetadata().getStatus() == SegmentUnitStatus.Broken) {
        logger.warn("segment unit is already in broken status {}", segId);
        throw new NoNeedToBeDeletingException();
      }
      if (syncPersist) {
        persistForce = asyncUpdateSegmentUnitMetadata(segId, null,
            SegmentUnitStatus.Deleting);
      } else {
        asyncUpdateSegmentUnitMetadata(segId, null, SegmentUnitStatus.Deleting);
      }
    } finally {
      segmentUnit.unlockStatus();
    }

    if (persistForce) {
      persistSegmentUnitMeta(segmentUnit.getSegmentUnitMetadata(), true);
    }
  }

  public Long getArchiveId() {
    return archiveMetadata.getArchiveId();
  }

  public synchronized SegmentUnit removeSegmentUnit(SegId segId) throws Exception {
    updateSegmentUnitMetadata(segId, null, SegmentUnitStatus.Deleted);

    SegmentUnit segmentUnit = segUnits.remove(segId);
    removeSegmentUnitFromEngines(segId);
    if (segmentUnit == null) {
      throw new IllegalArgumentException("seg id is unknown:" + segId);
    } else {
      return segmentUnit;
    }

  }

  private void dealWithSpecialStatus(SegId segId, SegmentUnitStatus newStatus) {
    if (newStatus == SegmentUnitStatus.Deleted) {
      removeSegmentUnitFromEngines(segId);
      SegmentUnit segmentUnit = segUnits.get(segId);
      freeBrickWhenSegmentUnitDeleted(segmentUnit);
      updateLogicalFreeSpaceToMetaData();
    } else if ((

        newStatus == SegmentUnitStatus.ModeratorSelected
            || newStatus == SegmentUnitStatus.SecondaryApplicant
            || newStatus == SegmentUnitStatus.Deleting || newStatus == SegmentUnitStatus.OFFLINED
            || newStatus == SegmentUnitStatus.Broken || newStatus == SegmentUnitStatus.Unknown)
        && catchupLogEngine != null) {
      logger.info("Going to pause the segId({}) PCL engine, new Status: {}", segId, newStatus);
      catchupLogEngine
          .pauseSegmentUnitProcessing(new CatchupLogContextKey(segId, CatchupLogDriverType.PCL));
      segUnits.get(segId).setPauseCatchUpLogForSecondary(true);
    }
  }

  private void removeSegmentUnitFromEngines(SegId segId) {
    if (catchupLogEngine != null) {
      catchupLogEngine.removeSegmentUnit(segId, callback);
    }
    if (stateProcessingEngine != null) {
      stateProcessingEngine.removeSegmentUnit(segId, callback);
    }
  }

  public List<SegId> removeAllSegmentUnits() {
    List<SegId> segIds = new ArrayList<>(segUnits.keySet());
    segUnits.clear();
    return segIds;
  }

  /**
   * get the real physical storage size.
   */
  public long getCapacity() {
    if (!canBeUsed()) {
      return 0L;
    } else {
      return storage.size();
    }
  }

  public long getLogicalSpace() {
    if (!canBeUsed()) {
      return 0L;
    } else {
      spaceLock.lock();
      try {
        return spaceManager.getTotalSpace();
      } finally {
        spaceLock.unlock();
      }
    }
  }

  public synchronized InstanceId getInstanceId() {
    return archiveMetadata.getInstanceId();
  }

  public synchronized void setInstanceId(InstanceId instanceId) {
    archiveMetadata.setInstanceId(instanceId);
  }

  public synchronized void setGroup(Group group) {
    archiveMetadata.setGroup(group);
  }

  public synchronized void clearInstanceId() throws JsonProcessingException, StorageException {
    archiveMetadata.setInstanceId(null);
    persistMetadata();
  }

  public RawArchiveMetadata getArchiveMetadata() {
    return archiveMetadata;
  }

  public SegmentUnit getSegmentUnit(SegId segId) {
    return segUnits.get(segId);
  }

  public synchronized void resetSegmentUnitStatus() {
    ArchiveStatus status = archiveMetadata.getStatus();
    Validate.isTrue(
        status == ArchiveStatus.GOOD || status == ArchiveStatus.DEGRADED
            || status == ArchiveStatus.OFFLINING);

    for (SegmentUnit segmentUnit : segUnits.values()) {
      SegmentUnitMetadata metadata = segmentUnit.getSegmentUnitMetadata();
      segmentUnit.lockStatus();
      boolean persistForce = false;
      try {
        SegmentUnitStatus currentStatus = metadata.getStatus();
        if (currentStatus != SegmentUnitStatus.Deleting
            && currentStatus != SegmentUnitStatus.Deleted) {
          if (metadata.getMigrationStatus().isMigratedStatus()) {
            logger.warn("datanode restart when the semgent is copying page, copy page status: {}",
                metadata.getMigrationStatus());
          }
          persistForce = asyncUpdateSegmentUnitMetadata(segmentUnit.getSegId(), null,
              SegmentUnitStatus.Start);
        }
      } catch (Exception e) {
        logger.warn("can't reset segment unit {} to Start status", segmentUnit, e);
      } finally {
        segmentUnit.unlockStatus();
      }

      if (persistForce) {
        segmentUnit.getArchive().persistSegmentUnitMeta(segmentUnit.getSegmentUnitMetadata(), true);
      }
    }
  }

  @Override
  public String toString() {
    return "RawArchive [archiveMetadata=" + archiveMetadata + ", segUnit=" + segUnits.keySet()
        .size() + ", starage="
        + storage + ", physicalSegmentUnitSize=" + physicalSegmentUnitSize + "]";
  }

  public void close() {
    try {
      this.persistBitMapOfShadowUnits();
      throw new RuntimeException();
    } catch (Exception e) {
      logger.warn("someone is closing the storage corresponding to archive {}",
          archiveMetadata.getArchiveId(),
          e);
    }

    try {
      this.storage.close();
    } catch (Exception e) {
      logger.warn("caught an exception when closing storage={}", storage, e);
    }
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((archiveMetadata == null) ? 0 : archiveMetadata.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    RawArchive other = (RawArchive) obj;
    if (archiveMetadata == null) {
      if (other.archiveMetadata != null) {
        return false;
      }
    } else if (!archiveMetadata.equals(other.archiveMetadata)) {
      return false;
    }
    return true;
  }

  public boolean canBeUsed() {
    ArchiveStatus status = this.archiveMetadata.getStatus();
    if (storage.isClosed() || storage.isBroken()) {
      logger.warn("archive status {}, isClosed={}, isBroken={}", status, storage.isClosed(),
          storage.isBroken());
      return false;
    }

    return (status == ArchiveStatus.DEGRADED || status == ArchiveStatus.GOOD
        || status == ArchiveStatus.OFFLINING);
  }

  public void addSegmentUnitsToCatchupLogEngine(SegmentUnitTaskExecutor catchupEngine,
      boolean pauseExistingSegmentUnits) {
    Validate.notNull(catchupEngine);
    catchupLogEngine = catchupEngine;
    for (SegId segId : segUnits.keySet()) {
      if (!segUnits.get(segId).isArbiter()) {
        catchupEngine.addSegmentUnit(segId, pauseExistingSegmentUnits);
      }
    }
  }

  public void addSegmentUnitsToStateProcessingEngine(SegmentUnitTaskExecutor stateProcessingEngine,
      boolean pauseExistingSegmentUnits) {
    Validate.notNull(stateProcessingEngine);
    this.stateProcessingEngine = stateProcessingEngine;
    for (SegId segId : segUnits.keySet()) {
      stateProcessingEngine.addSegmentUnit(segId, pauseExistingSegmentUnits);
    }
  }

  public SegmentUnitTaskExecutor getCatchupLogEngine() {
    return catchupLogEngine;
  }

  public void setCatchupLogEngine(SegmentUnitTaskExecutor catchupLogEngine) {
    this.catchupLogEngine = catchupLogEngine;
  }

  public SegmentUnitTaskExecutor getStateProcessingEngine() {
    return stateProcessingEngine;
  }

  public void setStateProcessingEngine(SegmentUnitTaskExecutor stateProcessingEngine) {
    this.stateProcessingEngine = stateProcessingEngine;
  }

  public ArchiveStatus getArchiveStatus() {
    return this.archiveMetadata.getStatus();
  }

  @Override
  public synchronized void setArchiveStatus(ArchiveStatus newArchiveStatus)
      throws ArchiveNotExistException, ArchiveStatusException {
    Validate.notNull(archiveMetadata);
    if (archiveMetadata.getStatus() == newArchiveStatus) {
      logger.warn("{},set same archive status {}", archiveMetadata, newArchiveStatus);
      if (archiveMetadata.getStatus() != ArchiveStatus.GOOD) {
        return;
      }
    }

    if (archiveStatusListers.size() == 0) {
      logger.warn("there is no raw archive listener for archive={}", getArchiveMetadata());
      Validate.isTrue(false);
    }

    logger.warn("set archive with new status {}, archive is {}", newArchiveStatus, archiveMetadata);
    ArchiveStatus currentStatus = archiveMetadata.getStatus();
    currentStatus.validate(newArchiveStatus);
    try {
      for (ArchiveStatusListener lister : archiveStatusListers) {
        switch (newArchiveStatus) {
          case GOOD:
            lister.becomeGood(this);
            break;
          case OFFLINING:
            lister.becomeOfflining(this);
            break;
          case OFFLINED:
            if (!isAllSegmentUnitOfflined()) {
              logger.warn("not all segment units are offlined for Archive={}", archiveMetadata);
              throw new ArchiveStatusException("fail to change status=" + newArchiveStatus);
            }
            lister.becomeOfflined(this);
            break;
          case DEGRADED:
            lister.becomeDegrade(this);
            break;
          case BROKEN:
            lister.becomeBroken(this);
            break;
          case CONFIG_MISMATCH:
            lister.becomeConfigMismatch(this);
            break;
          case INPROPERLY_EJECTED:
            lister.becomeInProperlyEjected(this);
            break;
          case EJECTED:
            lister.becomeEjected(this);
            break;
          default:
            break;
        }
        updateLogicalFreeSpaceToMetaData();
      }
    } catch (ArchiveNotExistException ne) {
      logger.error("archive not found in manager ", archiveMetadata);
      throw new ArchiveNotExistException("change status error");
    } catch (Exception e) {
      logger.error("change archive {} to status {} error ", archiveMetadata, newArchiveStatus, e);
      throw new ArchiveStatusException("change status error");
    }

  }

  /**
   * When all segment units are at offlined status, we consider their corresponding archive is
   * offlined.
   */
  public boolean isAllSegmentUnitOfflined() {
    boolean offlined = true;
    for (Entry<SegId, SegmentUnit> entry : segUnits.entrySet()) {
      if (entry.getValue().getSegmentUnitMetadata().getStatus() != SegmentUnitStatus.OFFLINED) {
        logger.warn("the current segmentunit {} is not offlined",
            entry.getValue().getSegmentUnitMetadata());
        offlined = false;
        break;
      }
    }

    return offlined;
  }

  /**
   * Recover segment unit from deleting.
   */
  public void recycleSegmentFromDeleting(SegId segId)
      throws SegmentUnitRecoverFromDeletingFailException {
    SegmentUnit segUnit = segUnits.get(segId);
    Validate.notNull(segUnit);

    if (segUnit.getSegmentUnitMetadata().getStatus() != SegmentUnitStatus.Deleting) {
      throw new SegmentUnitRecoverFromDeletingFailException(segId.getVolumeId().getId(),
          segId.getIndex(),
          "Segment unit is not in Deleting status");
    } else if (System.currentTimeMillis()
        > segUnit.getSegmentUnitMetadata().getLastUpdated()
        + RawArchive.msFromDeletingToDeletedForSegment
        - RawArchive.quarantineZoneMSToRecoverDeletingSegment) {
      logger.debug("current time {}, lastupdate {}, deletingToDeleting {}, quarantineZone {}",
          System.currentTimeMillis(), segUnit.getSegmentUnitMetadata().getLastUpdated(),
          RawArchive.msFromDeletingToDeletedForSegment,
          RawArchive.quarantineZoneMSToRecoverDeletingSegment);
      throw new SegmentUnitRecoverFromDeletingFailException(segId.getVolumeId().getId(),
          segId.getIndex(),
          "Segment unit is in quarantine zone");
    }

    try {
      this.updateSegmentUnitMetadata(segId, null, SegmentUnitStatus.Start);
    } catch (Exception e) {
      logger.error("update segment status to start failed: segment {}", segUnit, e);
      throw new SegmentUnitRecoverFromDeletingFailException(segId.getVolumeId().getId(),
          segId.getIndex(),
          e.getMessage());
    }

  }

  public void updateArchiveConfiguration() {
    this.physicalSegmentUnitSize = calculatePhysicalSegmentUnitSize(
        archiveMetadata.getSegmentUnitSize(),
        archiveMetadata.getPageSize());
  }

  public void setHostHeartbeat(HostHeartbeat hostHeartbeat) {
    this.hostHeartbeat = hostHeartbeat;
  }

  public Map<VolumeId, Set<SegId>> getVolumeToSegmentRelationship() {
    return volumeToSegmentRelationship;
  }

  public synchronized void persistBitMapOfShadowUnits() {
    if (this.canBeUsed()) {
      this.spaceManager.persistBitMap();
    }
  }

  @VisibleForTesting
  public AbstractSegmentUnitMetadata getArchiveUnit(long offset) {
    return this.spaceManager.getArchiveUnitByOffset(offset);
  }

  @VisibleForTesting
  public BrickMetadata getArchiveBrick(long offset) {
    return this.spaceManager.getArchiveBrickByOffset(offset);
  }

  public Collection<BrickMetadata> getAllBrickMetadata() {
    spaceLock.lock();
    try {
      return spaceManager.getAllBrickMetadata();
    } finally {
      spaceLock.unlock();
    }
  }

  public void recoverBitmapForSegmentUnit(LogPersistingConfiguration dataNodeLogPersistingCfg) {
    if (!this.canBeUsed()) {
      logger.warn("archvie {} can not be used, skip to recover bitmap for segment unit", this);
      return;
    }

    for (Entry<SegId, SegmentUnit> entry : this.segUnits.entrySet()) {
      recoverBitmapForSingleSegmentUnit(entry.getValue(), dataNodeLogPersistingCfg);
    }
  }

  private void recoverBitmapForSingleSegmentUnit(SegmentUnit segmentUnit,
      LogPersistingConfiguration dataNodeLogPersistingCfg) {
    SegmentUnitMetadata metadata = segmentUnit.getSegmentUnitMetadata();
    if (metadata.getMigrationStatus().isMigratedStatus()) {
      logger.warn("there is no need recovering bitmap for segId={}, status={}", metadata.getSegId(),
          metadata.getMigrationStatus());
      return;
    }

    long logId = metadata.getLogIdOfPersistBitmap();
    int maxNumberLog = dataNodeLogPersistingCfg.getMaxNumLogsInOneFile();
    Validate.notNull(this.logStorageReader);
    logger.warn("getLogIdOfPersistBitmap {}", metadata);

    try {
      do {
        List<MutationLogEntry> logs = logStorageReader
            .readLogsAfter(metadata.getSegId(), logId, maxNumberLog, false);
        for (MutationLogEntry log : logs) {
          if (log.getStatus() == LogStatus.Committed) {
            long offset = log.getOffset();
            int pageIndex = (int) (offset / ArchiveOptions.PAGE_SIZE);
            segmentUnit.setPageHasBeenWritten(pageIndex);
          }
        }

        if (logs.isEmpty()) {
          break;
        }

        logId = logs.get(logs.size() - 1).getLogId();
      } while (true);

    } catch (IOException | LogIdNotFoundException | KvStoreException e) {
      logger.error("catch an exception when read log from log system", e);
    }
  }

  public void setLogStorageReader(LogStorageReader logStorageReader) {
    this.logStorageReader = logStorageReader;
  }

  public synchronized void persistBitMapAndSegmentUnitIfNecessary() {
    if (!this.canBeUsed()) {
      return;
    }

    for (SegmentUnit segUnit : getSegmentUnits()) {
      SegmentUnitMetadata segmentUnitMetadata = segUnit.getSegmentUnitMetadata();
      try {
        if (segmentUnitMetadata.isBitmapNeedPersisted()) {
          logger.debug("now to persist bitmap and segment unit to storage");
          persistSegmentUnitMetaAndBitmap(segmentUnitMetadata);
          segmentUnitMetadata.setBitmapNeedPersisted(false);
        }
      } catch (Exception e) {
        logger
            .error("catch an exception when persist bit map or segment unit meta data to storage :",
                e);
      }
    }

    for (BrickMetadata brickMetadata : getAllBrickMetadata()) {
      try {
        if (brickMetadata.isBitmapNeedPersisted()) {
          logger.debug("now to persist bitmap and brick metadata to storage");
          persisBrickMetaAndBitmap(brickMetadata);
          brickMetadata.setBitmapNeedPersisted(false);
        }
      } catch (Exception e) {
        logger.error("catch an exception when persist bit map or brick metadata to storage :", e);
      }
    }
  }

  public Long getStoragePoolId() {
    return archiveMetadata.getStoragePoolId();
  }

  public void setStoragePoolId(Long storagePoolId) {
    archiveMetadata.setStoragePoolId(storagePoolId);
  }

  public boolean isOverloaded() {
    return archiveMetadata.isOverloaded();
  }

  public void setOverloaded(boolean overloaded) {
    archiveMetadata.setOverloaded(overloaded);
  }

  public void persistMetadata() throws JsonProcessingException, StorageException {
    ObjectMapper objectMapper = new ObjectMapper();
    logger.warn("some one persist the archiveMetadata {}", archiveMetadata);
    byte[] archiveMetadataBuf = objectMapper.writeValueAsBytes(archiveMetadata);
    ArchiveType archiveType = archiveMetadata.getArchiveType();
    if (!Archive.checkArchiveMetadataLength(archiveType, archiveMetadataBuf.length)) {
      logger.warn("metadata length={} is exceed max header length={}", archiveMetadataBuf.length,
          archiveType.getArchiveHeaderLength());
      throw new RuntimeException("");
    }

    int bufLen = archiveMetadataBuf.length + Archive.getMetadataOffset()
        + Archive.CHECKSUM_LENGTH_IN_ARCHIVE;
    ByteBuffer srcBuffer = ByteBuffer.allocate(bufLen);
    srcBuffer.putLong(archiveType.getMagicNumber());
    srcBuffer.putInt(archiveMetadataBuf.length);
    srcBuffer.put(archiveMetadataBuf);

    int length = bufLen - Archive.CHECKSUM_LENGTH_IN_ARCHIVE;

    srcBuffer.putLong(0L);
    srcBuffer.clear();

    TlsfByteBufferManager tlsfByteBufferManager = TlsfByteBufferManagerFactory.instance();
    Validate.notNull(tlsfByteBufferManager);

    ByteBuffer tempPagedAlignedBuffer = tlsfByteBufferManager
        .blockingAllocate(archiveType.getArchiveHeaderLength());

    try {
      tempPagedAlignedBuffer.put(srcBuffer.array()).clear();
      storage.write(archiveType.getArchiveHeaderOffset(), tempPagedAlignedBuffer);
      logger.debug("write archive metadata {} to disk successful", archiveMetadata);
    } finally {
      tlsfByteBufferManager.release(tempPagedAlignedBuffer);
    }
  }

  public boolean addMigrateFailedSegId(SegId segId) {
    return archiveMetadata.addMigrateFailedSegId(segId);
  }

  public List<SegId> drainAllMigrateFailedSegIds() {
    return archiveMetadata.drainAllMigrateFailedSegIds();
  }

  public void updateLastUserIoTime() {
    lastUserIoTime = System.currentTimeMillis();
  }

  public long getLastUserIoTime() {
    return lastUserIoTime;
  }

  public void markNotEnoughSpaceForSnapshot() {
    if (notEnoughSpaceForSnapshot.compareAndSet(false, true)) {
      logger.error("not enough space for snapshot, user's IO may be blocked {}", this);
    }
  }

  public void countLogs() {
    int numNotInsertedLogs = 0;
    int numLogsAfterPcl = 0;
    int numLogsFromPlalToPcl = 0;
    int numLogsFromPplToPlal = 0;
    for (SegmentUnit segmentUnit : segUnits.values()) {
      SegmentLogMetadata logMetadata = segmentUnit.getSegmentLogMetadata();
      if (logMetadata != null) {
        numNotInsertedLogs += logMetadata.getNotInsertedLogsCount();
        numLogsAfterPcl += logMetadata.getNumLogsAfterPcl();
        numLogsFromPlalToPcl += logMetadata.getNumLogsFromPlalToPcl();
        numLogsFromPplToPlal += logMetadata.getNumLogsFromPplToPlal();
      }
    }

    if (deviceName == null) {
      deviceName = Storage.getDeviceName(getStorage().identifier());
    }

    logger.warn(
        "{} notInsertedLogsCount {} numLogsAfterPcl {}, numLogsFromPlalToPcl {}, "
            + "numLogsFromPplToPlal {}",
        deviceName, numNotInsertedLogs, numLogsAfterPcl, numLogsFromPlalToPcl,
        numLogsFromPplToPlal);

  }

  private void freeBrickWhenSegmentUnitDeleted(SegmentUnit segmentUnit) {
    BrickMetadata brickMetadata = segmentUnit.getSegmentUnitMetadata().getBrickMetadata();
    if (brickMetadata == null) {
      return;
    }

    spaceLock.lock();
    try {
      brickMetadata.clear();

      int newIndex = 0 - Math.abs(segmentUnit.getSegId().getIndex());
      brickMetadata.setSegId(new SegId(segmentUnit.getVolumeId(), newIndex));
      segmentUnit.getSegmentUnitMetadata().setBrickMetadata(null);
    } catch (Exception e) {
      logger.error("fetch negative iterator failed", e);
    } finally {
      spaceLock.unlock();
    }

  }

  public PageAddress generatePageAddress(SegId segId, long physicalOffsetOnArchive) {
    long brickStartOffset = spaceManager.getBrickStartOffset(physicalOffsetOnArchive);
    return new PageAddressImpl(segId, brickStartOffset,
        physicalOffsetOnArchive - brickStartOffset, storage);
  }

  public void cleanUpTimeoutLogs(long cleanUpUuidPeriod, long cleanCompletingLogPeriod) {
    for (SegmentUnit segmentUnit : segUnits.values()) {
      if (segmentUnit.getSegmentUnitMetadata().getStatus().isStable()) {
        segmentUnit.getSegmentLogMetadata()
            .cleanUpTimeOutUuid(cleanUpUuidPeriod, cleanCompletingLogPeriod);
      }
    }
  }

}
