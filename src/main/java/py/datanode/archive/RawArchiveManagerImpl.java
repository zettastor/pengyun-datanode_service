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
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimap;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.app.context.AppContext;
import py.archive.Archive;
import py.archive.ArchiveOptions;
import py.archive.ArchiveStatus;
import py.archive.ArchiveStatusChangeFinishListener;
import py.archive.ArchiveStatusListener;
import py.archive.ArchiveType;
import py.archive.RawArchiveMetadata;
import py.archive.disklog.DiskErrorLogManager;
import py.archive.segment.SegId;
import py.archive.segment.SegmentUnitMetadata;
import py.archive.segment.SegmentUnitStatus;
import py.archive.segment.SegmentUnitType;
import py.archive.segment.recurring.SegmentUnitTaskCallback;
import py.archive.segment.recurring.SegmentUnitTaskExecutor;
import py.common.FastBufferManagerProxy;
import py.common.NamedThreadFactory;
import py.common.PrimaryTlsfFastBufferManager;
import py.consumer.ConsumerService;
import py.consumer.SingleThreadConsumerService;
import py.datanode.archive.selection.strategy.ArchiveSelectionStrategy;
import py.datanode.archive.selection.strategy.SegmentWrapperBasedRoundRobinArchiveSelectionStrategy;
import py.datanode.configuration.DataNodeConfiguration;
import py.datanode.configuration.LogPersistingConfiguration;
import py.datanode.exception.CannotAllocMoreArbiterException;
import py.datanode.exception.CannotAllocMoreFlexibleException;
import py.datanode.exception.InsufficientFreeSpaceException;
import py.datanode.exception.InvalidSegmentStatusException;
import py.datanode.exception.MultipleInstanceIdException;
import py.datanode.exception.StaleMembershipException;
import py.datanode.exception.StorageBrokenException;
import py.datanode.segment.ArbiterManager;
import py.datanode.segment.PersistDataContext;
import py.datanode.segment.PersistDataContextFactory;
import py.datanode.segment.PersistDataToDiskEngineImpl;
import py.datanode.segment.PersistDataType;
import py.datanode.segment.SegmentUnit;
import py.datanode.segment.SegmentUnitCanDeletingCheck;
import py.datanode.segment.SegmentUnitManager;
import py.datanode.segment.SegmentUnitMetadataAccessor;
import py.datanode.segment.datalog.MutationLogEntry;
import py.datanode.segment.datalog.MutationLogEntryFactory;
import py.datanode.segment.datalog.MutationLogManager;
import py.datanode.segment.datalog.SegmentLogMetadata;
import py.datanode.segment.datalog.persist.LogPersister;
import py.datanode.segment.datalog.sync.log.SyncLogTaskExecutor;
import py.datanode.segment.datalog.sync.log.driver.SecondaryPclDriver;
import py.datanode.segment.heartbeat.HostHeartbeat;
import py.exception.AllSegmentUnitDeadInArchviveException;
import py.exception.ArchiveIsNotCleanedException;
import py.exception.ArchiveNotExistException;
import py.exception.ArchiveNotFoundException;
import py.exception.ArchiveStatusException;
import py.exception.ArchiveTypeNotSupportException;
import py.exception.DiskBrokenException;
import py.exception.DiskDegradeException;
import py.exception.InvalidInputException;
import py.exception.NotEnoughSpaceException;
import py.exception.SegmentUnitBecomeBrokenException;
import py.exception.SegmentUnitRecoverFromDeletingFailException;
import py.exception.StorageException;
import py.instance.Group;
import py.instance.InstanceId;
import py.membership.SegmentMembership;
import py.netty.exception.TimeoutException;
import py.storage.Storage;
import py.storage.impl.AsynchronousFileChannelStorageFactory;
import py.storage.impl.PriorityStorageImpl;
import py.thrift.share.SetArchiveConfigRequest;
import py.volume.VolumeType;

public class RawArchiveManagerImpl implements RawArchiveManager, ArchiveStatusListener {
  private static final Logger logger = LoggerFactory.getLogger(RawArchiveManagerImpl.class);

  private final Map<Long, RawArchive> mapArchiveIdToArchive = new ConcurrentHashMap<>();

  //this is only the informationReport lister
  private final List<ArchiveStatusChangeFinishListener> archiveStatusReportListers =
      new CopyOnWriteArrayList<>();
  private final SegmentUnitManager segmentUnitManager;
  private final DiskErrorLogManager diskErrorLogManager;
  private final ArchiveSelectionStrategy<RawArchive> archiveSelectionStrategy;
  private final ExecutorService shadowPageAllocator;

  // lock which synchronize the segment unit allocation
  private final ReentrantLock allocateSegmentUnitLock = new ReentrantLock(true);
  // for sync log
  private final SyncLogTaskExecutor syncLogTaskExecutor;
  private final SegmentUnitCanDeletingCheck segmentUnitCanDeletingCheck;
  private final ScheduledExecutorService cleanUpPeriodExecutor;
  private ArbiterManager arbiterManager;
  private DataNodeConfiguration dataNodeCfg;
  private SegmentUnitTaskCallback unitTaskCallBack;
  // the group which this data node belong to
  private Group group;
  // the instanceId which data node belong to
  private InstanceId instanceId;
  private SegmentUnitTaskExecutor catchupLogEngine;
  private SegmentUnitTaskExecutor stateProcessingEngine;
  private MutationLogManager mutationLogManager;
  private LogPersister logPersister;
  private HostHeartbeat heartbeat;
  // store arbiter segment meta data.
  private RawArchive arbiterRawArchive;
  // a timer for pre-allocate bricks
  private SingleThreadConsumerService<RawArchivePreAllocator> preAllocateBrickExecutor;
  private AtomicBoolean shutdownFlag = new AtomicBoolean(false);

  public RawArchiveManagerImpl(SegmentUnitManager segmentUnitManager,
      Collection<Storage> storages,
      DiskErrorLogManager diskErrorLogManager, SegmentUnitTaskCallback unitTaskCallback,
      DataNodeConfiguration cfg, AppContext appContext, HostHeartbeat heartbeat,
      SyncLogTaskExecutor syncLogTaskExecutor,
      SegmentUnitCanDeletingCheck segmentUnitCanDeletingCheck) throws Exception {
    this(segmentUnitManager, storages, null, diskErrorLogManager, unitTaskCallback, cfg, appContext,
        heartbeat, syncLogTaskExecutor,
        segmentUnitCanDeletingCheck);

  }
  

  public RawArchiveManagerImpl(SegmentUnitManager segmentUnitManager,
      Collection<Storage> storages,
      Storage arbiterStorage, DiskErrorLogManager diskErrorLogManager,
      SegmentUnitTaskCallback unitTaskCallback,
      DataNodeConfiguration cfg, AppContext appContext, HostHeartbeat heartbeat,
      SyncLogTaskExecutor syncLogTaskExecutor,
      SegmentUnitCanDeletingCheck segmentUnitCanDeletingCheck) throws Exception {
    this.segmentUnitManager = segmentUnitManager;
    this.diskErrorLogManager = diskErrorLogManager;
    this.dataNodeCfg = cfg;
    this.unitTaskCallBack = unitTaskCallback;
    this.group = appContext.getGroup();
    this.instanceId = appContext.getInstanceId();
    this.heartbeat = heartbeat;
    this.archiveSelectionStrategy = new SegmentWrapperBasedRoundRobinArchiveSelectionStrategy(
        segmentUnitManager,
        instanceId);
    this.segmentUnitCanDeletingCheck = segmentUnitCanDeletingCheck;
    this.shadowPageAllocator = Executors
        .newSingleThreadExecutor(new NamedThreadFactory("shadow-page-allocator"));
    this.syncLogTaskExecutor = syncLogTaskExecutor;

    BlockingQueue<RawArchivePreAllocator> blockingQueue = new DelayQueue<>();
    preAllocateBrickExecutor = new SingleThreadConsumerService<>(RawArchivePreAllocator::run,
        blockingQueue, "brick-pre-allocator");

    RawArchive.setQuarantineZoneMsToRecoverDeletingSegment(
        dataNodeCfg.getQuarantineZoneMsToCreateDeletedSegment());
    RawArchive.setMsFromDeletingToDeletedForSegment(dataNodeCfg.getWaitTimeMsToMoveToDeleted());

    this.arbiterRawArchive = initArbiterRawArchive(arbiterStorage);
    addToSegmentUnitManager(arbiterRawArchive);
    Map<Long, RawArchive> mapNewArchivesBase = initValidArchives(storages);
    List<Archive> badArchives = new ArrayList<>();

    for (Map.Entry<Long, RawArchive> entry : mapNewArchivesBase.entrySet()) {
      RawArchive newArchive = entry.getValue();
      ArchiveType type = newArchive.getArchiveMetadata().getArchiveType();
      ArchiveStatus archiveStatus = newArchive.getArchiveStatus();
      newArchive.addListener(this);
      Validate.isTrue(
          archiveStatus != ArchiveStatus.EJECTED
              && archiveStatus != ArchiveStatus.INPROPERLY_EJECTED);

      if (!isArchiveMatchConfig(newArchive.getArchiveMetadata())) {
        logger
            .debug("put the config_mismatch archive to archive manager, archive is {}", newArchive);
        addArchive(newArchive.getArchiveId(), newArchive);
        newArchive.setArchiveStatus(ArchiveStatus.CONFIG_MISMATCH);
        continue;
      }

      if (diskErrorLogManager != null) {
        try {
          logger.warn("check the archive {}", newArchive);
          diskErrorLogManager.putArchive(newArchive);
        } catch (DiskBrokenException e) {
          logger.error(
              "RawArchive has more than the threshold of storage exceptions. " 
                  + "Set it status to Broken. archive: {}",
              newArchive.getArchiveId(), e);
          for (SegmentUnit segmentUnit : newArchive.getSegmentUnits()) {
            newArchive.markSegmentUnitDeleting(segmentUnit.getSegId(), false);
          }

          addToSegmentUnitManager(newArchive);
          logger.debug("put the broken archive to archive manager, archive is {}", newArchive);
          addArchive(newArchive.getArchiveId(), newArchive);
          newArchive.setArchiveStatus(ArchiveStatus.BROKEN);
          continue;
        } catch (DiskDegradeException e) {
          if (archiveStatus == ArchiveStatus.GOOD) {
            logger.error(
                "RawArchive has occured I/O exception. Set it status to Degrade. archive: {}",
                newArchive.getArchiveId(), e);
            newArchive.getArchiveMetadata().setStatus(ArchiveStatus.DEGRADED);
          }
          newArchive.resetSegmentUnitStatus();
        }

        if (archiveStatus == ArchiveStatus.GOOD || archiveStatus == ArchiveStatus.DEGRADED) {
          newArchive.resetSegmentUnitStatus();
        }
      }

      if (archiveStatus == ArchiveStatus.GOOD || archiveStatus == ArchiveStatus.DEGRADED) {
        logger.debug("archive status is {}, set logical space {}", archiveStatus,
            newArchive.getLogicalSpace());
        newArchive.getArchiveMetadata().setLogicalSpace(newArchive.getLogicalSpace());
      }

      archiveStatus = newArchive.getArchiveStatus();
      if (archiveStatus == ArchiveStatus.OFFLINING || archiveStatus == ArchiveStatus.OFFLINED
          || archiveStatus == ArchiveStatus.GOOD || archiveStatus == ArchiveStatus.DEGRADED
          || archiveStatus == ArchiveStatus.BROKEN) {
        addToSegmentUnitManager(newArchive);
      }

      if (archiveStatus != ArchiveStatus.CONFIG_MISMATCH) {
        newArchive.setGroup(this.group);
        newArchive.setInstanceId(this.instanceId);
        logger.debug(
            "current archive status is not config_mismatch, persist instanceId and groupID, {}",
            newArchive);
        newArchive.persistMetadata();
      }

      if (!archiveStatus.canDoIo()) {
        logger.warn(
            "current archive {} can not do io, status is not good, degraded, " 
                + "or offlining, close storage",
            newArchive);
        badArchives.add(newArchive);
      }

      logger.warn("put the archive to archive manager, archive is {}", newArchive);
      addArchive(newArchive.getArchiveId(), newArchive);
      int ratio = dataNodeCfg.getPrimaryFastBufferPercentage() + dataNodeCfg
          .getSecondaryFastBufferPercentage();
      long memorySize = dataNodeCfg.getMemorySizeForDataLogsMb();
      FastBufferManagerProxy fastBufferManagerProxy = new FastBufferManagerProxy(
          new PrimaryTlsfFastBufferManager(dataNodeCfg.getFastBufferAlignmentBytes(),
              memorySize * ratio / 100 * 1024L * 1024L),
          Storage.getDeviceName(newArchive.getStorage().identifier()));
      MutationLogEntryFactory
          .addFastBufferManager(newArchive.getArchiveId(), fastBufferManagerProxy);
    }

    for (Archive archive : badArchives) {
      if (archive.getStorage() != null) {
        archive.getStorage().close();
      }
    }

    cleanUpPeriodExecutor = Executors
        .newScheduledThreadPool(dataNodeCfg.getCoreOfCleanUpTimeoutLogsExecturoThreads(),
            new NamedThreadFactory("mutation-log-clean-up"));
    for (RawArchive rawArchive : mapArchiveIdToArchive.values()) {
      if (rawArchive.canBeUsed()) {
        cleanUpPeriodExecutor.scheduleAtFixedRate(new Runnable() {
          @Override
          public void run() {
            if (rawArchive.canBeUsed()) {
              rawArchive.cleanUpTimeoutLogs(
                  dataNodeCfg.getTimeoutOfCleanUpCrashOutUuidMs(),
                  dataNodeCfg.getTimeoutOfCleanUpCompletingLogsMs());
            }
          }
        }, 0, dataNodeCfg.getPeriodCleanUpTimeoutLogsPerArchive(), TimeUnit.MILLISECONDS);
      }
    }

    startPreAllocator();

    RawArchivePreAllocator preAllocator = new RawArchivePreAllocator(arbiterRawArchive,
        dataNodeCfg, preAllocateBrickExecutor, true);
    preAllocateBrickExecutor.submit(preAllocator);
    for (RawArchive archive : mapNewArchivesBase.values()) {
      preAllocator = new RawArchivePreAllocator(archive,
          dataNodeCfg, preAllocateBrickExecutor);
      preAllocateBrickExecutor.submit(preAllocator);
    }
  }

  public SegmentUnitManager getSegmentUnitManager() {
    return segmentUnitManager;
  }

  private void handleUnfinalSegmenUnitStatus(RawArchive archive, SegmentUnit segmentUnit) {
    SegmentUnitMetadata unitMetadata = segmentUnit.getSegmentUnitMetadata();

    SegmentUnitStatus status = unitMetadata.getStatus();
    if (!status.isFinalStatus() && status != SegmentUnitStatus.Start) {
      logger.warn("set status {} to Start, segment  is {}", status, unitMetadata);
      try {
        archive.updateSegmentUnitMetadata(unitMetadata.getSegId(), null, SegmentUnitStatus.Start);
      } catch (InvalidSegmentStatusException | StaleMembershipException e) {
        logger.error("can not update the segment unit={}, archive metadata={}", unitMetadata,
            archive.getArchiveMetadata());

        throw new RuntimeException(
            "can not build the segment unit information when initializing archive");
      }
    }
  }

  private void addToSegmentUnitManager(RawArchive archive) throws Exception {
    Collection<SegmentUnit> segmentUnits = archive.getSegmentUnits();
    for (SegmentUnit segUnit : segmentUnits) {
      handleUnfinalSegmenUnitStatus(archive, segUnit);

      SegId segId = segUnit.getSegId();
      SegmentUnit existingSegmentUnit = segmentUnitManager.get(segId);
      if (existingSegmentUnit == null) {
        segmentUnitManager.put(segUnit);
      } else {
        SegmentMembership myMembership = segUnit.getSegmentUnitMetadata().getMembership();
        SegmentMembership existingMembership = existingSegmentUnit.getSegmentUnitMetadata()
            .getMembership();
        if (myMembership.compareTo(existingMembership) > 0) {
          segmentUnitManager.remove(segId);
          existingSegmentUnit.getArchive().removeSegmentUnit(segId);
          segmentUnitManager.put(segUnit);
        } else {
          segUnit.getArchive().removeSegmentUnit(segId);
        }
      }
    }
  }

  private Storage generateArbiterStorage() {
    AsynchronousFileChannelStorageFactory factory = AsynchronousFileChannelStorageFactory
        .getInstance();
    File file = new File(dataNodeCfg.getArbiterStoreFile());
    if (!file.exists()) {
      try {
        file.createNewFile();
      } catch (IOException e) {
        logger.warn("create new file:{} exception.", dataNodeCfg.getArbiterStoreFile(), e);
      }
    }
    Storage arbitStorage = null;
    try {
      arbitStorage = factory.generate(dataNodeCfg.getArbiterStoreFile());
      arbitStorage.open();
      arbitStorage.write(dataNodeCfg.getArbiterCountLimitInOneDatanode()
              * ArchiveOptions.SEGMENTUNIT_DESCDATA_LENGTH,
          ByteBuffer.allocate((int) ArchiveOptions.SEGMENTUNIT_DESCDATA_LENGTH));
    } catch (StorageException e) {
      logger.error("generate arbiter storage erro. file:{}", dataNodeCfg.getArbiterStoreFile(), e);
    }

    return arbitStorage;

  }

  private RawArchive initArbiterRawArchive(Storage storage) throws StorageException {
    boolean initArchive = false;
    RawArchive rawArchive = null;

    File file = new File(dataNodeCfg.getArbiterStoreFile());
    if (!file.exists()) {
      initArchive = true;
    }

    if (storage == null) {
      storage = generateArbiterStorage();
    }

    if (storage.size() < ArchiveOptions.SEGMENTUNIT_METADATA_LENGTH) {
      initArchive = true;
    }

    if (storage == null) {
      logger.error(" arbiter storage is null, so can't generate raw archive.");
      return null;
    }

    try {
      RawArchiveBuilder rawArchiveBuilder = new RawArchiveBuilder(dataNodeCfg, storage)
          .setSegmentUnitTaskCallback(unitTaskCallBack);
      rawArchiveBuilder.setSyncLogTaskExecutor(syncLogTaskExecutor);
      if (initArchive) {
        rawArchiveBuilder.setArchiveType(ArchiveType.RAW_DISK);
        rawArchive = (RawArchive) rawArchiveBuilder.initArbiterArchive(null);
      } else {
        rawArchive = (RawArchive) rawArchiveBuilder.reLoadArbiterArchive();
      }
    } catch (Exception e) {
      logger.error("caught an exception when loading the storage={}", storage, e);
      storage.close();
    }
    return rawArchive;
  }

  private Map<Long, RawArchive> initValidArchives(Collection<Storage> newStorages)
      throws Exception {
    Map<Long, RawArchive> addedArchives = new HashMap<>();
    for (Storage storage : newStorages) {
      RawArchive archive = null;
      try {
        RawArchiveBuilder rawArchiveBuilder = new RawArchiveBuilder(dataNodeCfg, storage)
            .setSegmentUnitTaskCallback(unitTaskCallBack)
            .setArchiveBuilderCallback(
                archiveMetadata -> isArchiveMatchConfig((RawArchiveMetadata) archiveMetadata));
        rawArchiveBuilder.setSyncLogTaskExecutor(syncLogTaskExecutor);
        archive = (RawArchive) rawArchiveBuilder.build();
      } catch (Exception e) {
        logger.error("caught an exception when loading the storage={}", storage, e);
        storage.close();
        continue;
      }

      logger.warn("archive read from disk is {}, archive size {}", archive,
          archive.getStorage().size());

      long archiveId = archive.getArchiveId();
      RawArchive existingArchive = getRawArchive(archiveId);
      if (existingArchive != null) {
        String newSerialNum = archive.getArchiveMetadata().getSerialNumber();
        String oldSerialNum = existingArchive.getArchiveMetadata().getSerialNumber();
        if (newSerialNum != null && !newSerialNum.equals(oldSerialNum)) {
          if (existingArchive.getArchiveMetadata().getDeviceName().equalsIgnoreCase(oldSerialNum)) {
            logger.warn("exist archive: {}, new archive: {}", existingArchive,
                archive.getArchiveMetadata());
          } else {
            storage.close();
            throw new RuntimeException("Two archives have same archiveId: " + archiveId
                + " but different serial number: existed serial number " + existingArchive
                .getArchiveMetadata().getSerialNumber() + " newly plugged serial number: " + archive
                .getArchiveMetadata().getSerialNumber());
          }
        } else {
          logger.warn(
              "the archive has been plugged out and then plugged in again. archive metadata is {}",
              archive.getArchiveMetadata());
        }
      }
      addedArchives.put(archiveId, archive);
    }
    return addedArchives;
  }

  public List<RawArchive> getRawArchives() {
    List<RawArchive> archives = new ArrayList<>();
    for (Map.Entry<Long, RawArchive> entry : mapArchiveIdToArchive.entrySet()) {
      archives.add(entry.getValue());
    }
    return archives;
  }

  public List<RawArchive> getAllRawArchivesAndArbiterArchive() {
    List<RawArchive> archives = getRawArchives();
    if (arbiterRawArchive != null) {
      archives.add(arbiterRawArchive);
    }
    return archives;
  }

  public void startPreAllocator() {
    preAllocateBrickExecutor.start();
  }

  @Override
  public SegmentUnit createSegmentUnit(SegId segId, SegmentMembership membership,
      int segmentWrapSize, VolumeType volumeType, Long storagePoolId,
      SegmentUnitType segmentUnitType,
      String volumeMetadataJson, String accountMetadataJson,
      boolean isSecondaryCandidate, InstanceId replacee)
      throws InsufficientFreeSpaceException, Exception {
    return addSegmentUnit(segId, membership, segmentWrapSize, volumeType,
        volumeMetadataJson,
        accountMetadataJson,  false, storagePoolId, segmentUnitType, 0L, null,
        isSecondaryCandidate, replacee);
  }

  @Override
  public SegmentUnit createSegmentUnit(SegId segId, SegmentMembership membership,
      int segmentWrapSize,
      VolumeType volumeType,  Long storagePoolId,
      SegmentUnitType segmentUnitType,
      String volumeMetadataJson, String accountMetadataJson,
      long srcVolumeId, SegmentMembership srcMembership,
      boolean isSecondaryCandidate, InstanceId replacee) throws  Exception {
    return addSegmentUnit(segId, membership, segmentWrapSize, volumeType,
        volumeMetadataJson,
        accountMetadataJson,  false, storagePoolId, segmentUnitType,
        srcVolumeId,
        srcMembership, isSecondaryCandidate, replacee);
  }

  private SegmentUnit addSegmentUnit(SegId segId, SegmentMembership membership, int segmentWrapSize,
      VolumeType volumeType, String volumeMetadataJson,
      String accountMetadataJson,
      boolean creating, Long storagePoolId,
      SegmentUnitType segmentUnitType,
      long srcVolumeId, SegmentMembership srcMembership,
      boolean isSecondaryCandidate, InstanceId replacee)
      throws InsufficientFreeSpaceException,
      NotEnoughSpaceException, StorageException, StorageBrokenException, 
      CannotAllocMoreArbiterException, IOException, CannotAllocMoreFlexibleException {
    RawArchive archive = null;

    logger
        .info("new segment unit: {} added, storagePoolId: {}, membership: {}", segId, storagePoolId,
            membership);

    if (segmentUnitType == SegmentUnitType.Arbiter) {
      if (!canAllocMoreArbiter()) {
        throw new CannotAllocMoreArbiterException();
      }
    }

    SegmentUnit segmentUnit = null;
    allocateSegmentUnitLock.lock();
    try {
      if (segmentUnitType == SegmentUnitType.Arbiter) {
        archive = arbiterRawArchive;
      } else {
        archive = archiveSelectionStrategy
            .selectArchive(getRawArchives(), storagePoolId, segId.getVolumeId(), segId.getIndex(),
                segmentWrapSize, segmentUnitType, membership);
      }
      segmentUnit = archive
          .addSegmentUnit(segId, membership, volumeType, volumeMetadataJson,
              accountMetadataJson, segmentUnitType, srcVolumeId,
              srcMembership, isSecondaryCandidate, replacee);

    } finally {
      allocateSegmentUnitLock.unlock();
    }
    logger.warn("allocated a segment unit from archive : {}, segment unit {}",
        archive.getArchiveMetadata(),
        segmentUnit);
    if (creating) {
      segmentUnit.setSegmentUnitCreateDone(false);
    }
    return segmentUnit;
  }

  private boolean canAllocMoreArbiter() {
    return (arbiterManager.getCountOfArbiters() + 1) <= dataNodeCfg
        .getArbiterCountLimitInOneDatanode();
  }

  // Don't add the segment unit to CatchupLogEngine and StateProcessing engine yet
  // we need to prepare SegmentLogMetadata first
  @Override
  public SegmentUnit removeSegmentUnit(SegId segId, boolean removeFromSegmentUnitManager)
      throws Exception {
    RawArchive archive = segmentUnitManager.getArchive(segId);

    SegmentUnit unit = archive.removeSegmentUnit(segId);
    if (unit.getSegmentUnitMetadata().getSegmentUnitType() != SegmentUnitType.Arbiter) {
      archiveSelectionStrategy
          .removeSegmentUnit(unit, unit.getArchive());
    }
    if (removeFromSegmentUnitManager) {
      logger.warn("remove from the segment unit manager {}", segId);
      segmentUnitManager.remove(segId);
    }
    return unit;
  }

  @Override
  public SegmentUnit removeSegmentUnit(SegId segId) throws Exception {
    return removeSegmentUnit(segId, true);
  }

  public boolean updateSegmentUnitMetadata(SegId segId, String volumeMetadataJson)
      throws Exception {
    return updateSegmentUnitMetadata(segId, null, null, false, volumeMetadataJson);
  }

  public boolean updateSegmentUnitMetadata(SegId segId, SegmentMembership membership,
      SegmentUnitStatus status) throws Exception {
    return updateSegmentUnitMetadata(segId, membership, status, false, null);
  }

  public boolean updateSegmentUnitMetadata(SegId segId, SegmentMembership membership,
      SegmentUnitStatus status, String volumeMetadataJson) throws Exception {
    return updateSegmentUnitMetadata(segId, membership, status, false, volumeMetadataJson);
  }

  public boolean updateSegmentUnitMetadata(SegId segId, SegmentMembership membership,
      SegmentUnitStatus status, boolean acceptStaleMembership) throws Exception {
    return updateSegmentUnitMetadata(segId, membership, status, acceptStaleMembership, null);
  }

  public boolean updateSegmentUnitMetadata(SegId segId, SegmentMembership membership,
      SegmentUnitStatus newStatus, boolean acceptStaleMembership, String volumeMetadataJson)
      throws Exception {
    return updateSegmentUnitMetadata(segId, membership, newStatus, acceptStaleMembership,
        volumeMetadataJson, false);
  }

  public boolean updateSegmentUnitMetadata(SegId segId, SegmentMembership membership,
      SegmentUnitStatus newStatus,
      boolean acceptStaleMembership, String volumeMetadataJson, boolean persistAnyway)
      throws Exception {
    RawArchive archive = segmentUnitManager.getArchive(segId);
    return archive
        .updateSegmentUnitMetadata(segId, membership, newStatus, acceptStaleMembership,
            volumeMetadataJson,
            persistAnyway, true);
  }

  @Override
  public boolean asyncUpdateSegmentUnitMetadata(SegId segId, SegmentMembership membership,
      SegmentUnitStatus newStatus) throws Exception {
    RawArchive archive = segmentUnitManager.getArchive(segId);
    return archive.asyncUpdateSegmentUnitMetadata(segId, membership, newStatus);
  }

  @Override
  public boolean asyncUpdateSegmentUnitMetadata(SegId segId, SegmentMembership membership,
      SegmentUnitStatus newStatus, String volumeMetadataJson) throws Exception {
    RawArchive archive = segmentUnitManager.getArchive(segId);
    return archive
        .updateSegmentUnitMetadata(segId, membership, newStatus, false, volumeMetadataJson,
            false, false);
  }

  /**
   * returns the size of the largest contiguous free space block on any archive.
   */
  public long getLargestContiguousFreeSpace() {
    long result = 0;
    for (RawArchive archive : getRawArchives()) {
      result = Math.max(result, archive.getLargestContiguousLogicalFreeSpace());
    }
    return result;
  }

  public long getTotalLogicalFreeSpace() {
    long result = 0;
    for (RawArchive archive : getRawArchives()) {
      result += archive.getLogicalFreeSpace();
    }
    return result;
  }

  /**
   * returns the total capacity of this instance.
   */
  public long getTotalPhysicalCapacity() {
    long result = 0;

    for (RawArchive archive : getRawArchives()) {
      result += archive.getCapacity();
    }
    return result;
  }

  public long getTotalLogicalSpace() {
    long result = 0;
    for (RawArchive archive : getRawArchives()) {
      result += archive.getLogicalSpace();
    }
    return result;
  }

  public DiskErrorLogManager getDiskLog() {
    return diskErrorLogManager;
  }

  public RawArchive removeArchive(long archiveId, boolean needToCloseStorageThreadPool)
      throws ArchiveIsNotCleanedException {
    RawArchive archive = mapArchiveIdToArchive.get(archiveId);
    if (archive == null) {
      return null;
    }
    isAllSegmentUnitDeleted(archive);
    logger.warn("remove archive {}", archive);
    mapArchiveIdToArchive.remove(archiveId);
    diskErrorLogManager.removeArchive(archive);
    return archive;
  }

  @Override
  public InstanceId getInstanceIdOnArchives() throws MultipleInstanceIdException {
    Multimap<InstanceId, Archive> mapInstanceIdToArchive = LinkedHashMultimap.create();

    for (RawArchive archive : mapArchiveIdToArchive.values()) {
      InstanceId id = archive.getArchiveMetadata().getInstanceId();
      if (id == null) {
        continue;
      }

      mapInstanceIdToArchive.put(id, archive);
    }

    if (mapInstanceIdToArchive.size() == 0) {
      return null;
    }

    Set<InstanceId> instanceIds = mapInstanceIdToArchive.keySet();
    if (instanceIds.size() == 1) {
      InstanceId instanceId = instanceIds.iterator().next();
      logger.info("Found an instanceId {} on archives. numArchivesTotal: {} ", instanceId,
          mapArchiveIdToArchive.size());
      return instanceId;
    }

    throw new MultipleInstanceIdException(mapInstanceIdToArchive.toString());
  }

  public void close() {
    shutdownThread();
    logger.warn("start to close archive");
    for (RawArchive archive : mapArchiveIdToArchive.values()) {
      logger.warn("Closing archive: {} ", archive);
      archive.close();
    }

    arbiterRawArchive.close();
  }

  public void shutdownThread() {
    logger.warn("start shut down shadowPageAllocator");
    shutdownFlag.set(true);
    shadowPageAllocator.shutdown();
    try {
      if (!shadowPageAllocator.awaitTermination(30, TimeUnit.SECONDS)) {
        shadowPageAllocator.shutdownNow();
      }
    } catch (InterruptedException e) {
      logger.warn("shut down shadowPageAllocator, exception, now invoke shutdownNow.", e);
      shadowPageAllocator.shutdownNow();
    }

    preAllocateBrickExecutor.stop();
    cleanUpPeriodExecutor.shutdown();
    try {
      if (!cleanUpPeriodExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
        cleanUpPeriodExecutor.shutdownNow();
      }
    } catch (InterruptedException e) {
      logger.warn("shut down cleanUpPeriodExecutor, exception, now invoke shutdownNow.", e);
      cleanUpPeriodExecutor.shutdownNow();
    }
  }

  public void setCatchupLogEngine(SegmentUnitTaskExecutor catchupLogEngine) {
    this.catchupLogEngine = catchupLogEngine;

    for (RawArchive archive : getRawArchives()) {
      ArchiveStatus status = archive.getArchiveStatus();
      if (status == ArchiveStatus.GOOD || status == ArchiveStatus.DEGRADED
          || status == ArchiveStatus.OFFLINED
          || status == ArchiveStatus.OFFLINING || status == ArchiveStatus.BROKEN) {
        archive.addSegmentUnitsToCatchupLogEngine(catchupLogEngine, true);
      }
    }
  }

  public void setStateProcessingEngine(SegmentUnitTaskExecutor stateProcessingEngine) {
    this.stateProcessingEngine = stateProcessingEngine;

    for (RawArchive archive : getAllRawArchivesAndArbiterArchive()) {
      ArchiveStatus status = archive.getArchiveStatus();
      if (status == ArchiveStatus.GOOD || status == ArchiveStatus.DEGRADED
          || status == ArchiveStatus.OFFLINED
          || status == ArchiveStatus.OFFLINING || status == ArchiveStatus.BROKEN) {
        archive.addSegmentUnitsToStateProcessingEngine(stateProcessingEngine, false);
      }
    }
  }

  public void setGroup(Group group) throws Exception {
    Validate.isTrue(this.group == null);
    Validate.notNull(group);

    logger.warn("set current group to {}", group);
    this.group = group;

    for (RawArchive archive : getRawArchives()) {
      if (archive.getArchiveStatus() != ArchiveStatus.CONFIG_MISMATCH) {
        archive.setGroup(group);
        archive.persistMetadata();
      }
    }
  }

  @Override
  public void addArchiveStatusChangeFinishListener(ArchiveStatusChangeFinishListener listener) {
    boolean foundListener = false;
    for (ArchiveStatusChangeFinishListener listenerExist : archiveStatusReportListers) {
      if (listenerExist == listener) {
        foundListener = true;
      }
    }
    if (!foundListener) {
      this.archiveStatusReportListers.add(listener);
    }
  }

  @Override
  public void clearArchiveStatusChangeFinishListener() {
    archiveStatusReportListers.clear();
  }

  @Override
  public RawArchive getRawArchive(long archiveId) {
    RawArchive archive = mapArchiveIdToArchive.get(archiveId);
    return archive;
  }

  /**
   * This interface get the archive by serial number: Sometimes it need to check same serial number
   * has existed.
   */
  private RawArchive getArchiveBySerialNumber(String serialNum) {
    for (RawArchive archive : this.mapArchiveIdToArchive.values()) {
      String archiveSerialNum = archive.getArchiveMetadata().getSerialNumber();
      if (archiveSerialNum != null && archiveSerialNum.equals(serialNum)) {
        return archive;
      }
    }
    return null;
  }

  @Override
  public void becomeGood(Archive archive)
      throws ArchiveNotExistException, StorageException, JsonProcessingException,
      InterruptedException, TimeoutException {
    RawArchive rawArchive = this.getRawArchive(archive.getArchiveMetadata().getArchiveId());
    if (rawArchive == null) {
      logger.warn("archive={} not found", archive);
      throw new ArchiveNotExistException("archive: " + archive + "does not exist!");
    }

    logger.warn("existing the archive={}", rawArchive);
    final ArchiveStatus oldArchiveStatus = rawArchive.getArchiveStatus();

    if (oldArchiveStatus != ArchiveStatus.OFFLINING) {
      rawArchive.getStorage().open();
    }

    if (rawArchive.getArchiveStatus() == ArchiveStatus.CONFIG_MISMATCH) {
      this.correctConfigMismatchedArchive(rawArchive);
    }

    rawArchive.getArchiveMetadata().setStatus(ArchiveStatus.GOOD);
    rawArchive.getArchiveMetadata().setLogicalSpace(rawArchive.getLogicalSpace());
    rawArchive.updateLogicalFreeSpaceToMetaData();
    rawArchive.resetSegmentUnitStatus();

    rawArchive.setGroup(group);
    rawArchive.setInstanceId(instanceId);

    rawArchive.persistMetadata();

    for (ArchiveStatusChangeFinishListener listener : archiveStatusReportListers) {
      listener.becomeGood(rawArchive, oldArchiveStatus);
    }
  }

  @Override
  public void becomeConfigMismatch(Archive archive) throws ArchiveNotExistException {
    RawArchive rawArchive = this.getRawArchive(archive.getArchiveMetadata().getArchiveId());
    if (rawArchive == null) {
      throw new ArchiveNotExistException("archive: " + archive + "does not exist!");
    }
    final ArchiveStatus oldArchiveStatus = rawArchive.getArchiveStatus();
    rawArchive.getArchiveMetadata().setStatus(ArchiveStatus.CONFIG_MISMATCH);
    rawArchive.removeAllSegmentUnits();
    rawArchive.getArchiveMetadata().setLogicalSpace(0L);
    rawArchive.updateLogicalFreeSpaceToMetaData();
    rawArchive.close();

    for (ArchiveStatusChangeFinishListener listener : archiveStatusReportListers) {
      listener.becomeConfigMismatch(rawArchive, oldArchiveStatus);
    }
    logger.warn("set archive {} to CONFIG_MISMATCH status", rawArchive);
  }

  @Override
  public void becomeOfflining(Archive archive)
      throws ArchiveNotExistException, JsonProcessingException, StorageException {
    RawArchive rawArchive = this.getRawArchive(archive.getArchiveMetadata().getArchiveId());
    if (rawArchive == null) {
      throw new ArchiveNotExistException("archive: " + archive + "does not exist!");
    }
    final ArchiveStatus oldArchiveStatus = rawArchive.getArchiveStatus();
    Validate.isTrue(
        oldArchiveStatus == ArchiveStatus.GOOD || oldArchiveStatus == ArchiveStatus.DEGRADED);

    Collection<SegmentUnit> segUnitMetadatas = rawArchive.getSegmentUnits();
    for (SegmentUnit seg : segUnitMetadatas) {
      seg.lockStatus();
      boolean persistForce = false;
      try {
        SegmentUnitStatus segStatus = seg.getSegmentUnitMetadata().getStatus();

        if (segStatus != SegmentUnitStatus.Broken && segStatus != SegmentUnitStatus.Deleting
            && segStatus != SegmentUnitStatus.Deleted) {
          persistForce = rawArchive
              .asyncUpdateSegmentUnitMetadata(seg.getSegId(), null, SegmentUnitStatus.OFFLINING);
        }
      } catch (Exception e) {
        logger.error("catch an exception when updata segment unit to offling", e);
      } finally {
        seg.unlockStatus();
      }

      if (persistForce) {
        rawArchive.persistSegmentUnitMeta(seg.getSegmentUnitMetadata(), true);
      }
    }

    rawArchive.getArchiveMetadata().setStatus(ArchiveStatus.OFFLINING);

    rawArchive.updateLogicalFreeSpaceToMetaData();

    rawArchive.persistMetadata();

    for (ArchiveStatusChangeFinishListener listener : archiveStatusReportListers) {
      listener.becomeOfflining(rawArchive, oldArchiveStatus);
    }

    return;
  }

  @Override
  public void becomeOfflined(Archive archive)
      throws ArchiveNotExistException, JsonProcessingException, StorageException,
      ArchiveStatusException, ArchiveIsNotCleanedException {
    RawArchive rawArchive = this.getRawArchive(archive.getArchiveMetadata().getArchiveId());
    if (rawArchive == null) {
      throw new ArchiveNotExistException("archive: " + archive + "does not exist in system");
    }
    final ArchiveStatus oldArchiveStatus = rawArchive.getArchiveStatus();

    for (SegmentUnit segmentUnit : rawArchive.getSegmentUnits()) {
      SegmentUnitMetadata metadata = segmentUnit.getSegmentUnitMetadata();
      if (metadata.getStatus() != SegmentUnitStatus.OFFLINED) {
        Validate.isTrue(false,
            "segment unit=" + metadata + " is not offlined on archive=" + rawArchive);
      }

      try {
        PersistDataContext context = PersistDataContextFactory
            .generatePersistDataContext(metadata, PersistDataType.SegmentUnitMetadata);
        PersistDataToDiskEngineImpl.getInstance().syncPersistData(context);
      } catch (Exception e) {
        logger.warn("write segment unit catch an exception: segUnit {}", metadata, e);
      }
    }

    rawArchive.persistBitMapOfShadowUnits();

    rawArchive.updateLogicalFreeSpaceToMetaData();
    rawArchive.getArchiveMetadata().setStatus(ArchiveStatus.OFFLINED);
    rawArchive.getArchiveMetadata().setLogicalSpace(0L);
    rawArchive.updateLogicalFreeSpaceToMetaData();

    rawArchive.persistMetadata();

    logger.warn("archive is offlined {}, and close the storage", rawArchive);
    rawArchive.close();

    for (ArchiveStatusChangeFinishListener listener : archiveStatusReportListers) {
      listener.becomeOfflined(rawArchive, oldArchiveStatus);
    }
  }

  @Override
  public void becomeInProperlyEjected(Archive archive) throws ArchiveNotExistException {
    RawArchive rawArchive = this.getRawArchive(archive.getArchiveMetadata().getArchiveId());
    if (rawArchive == null) {
      throw new ArchiveNotExistException("archive: " + archive + "does not exist!");
    }
    final ArchiveStatus oldArchiveStatus = rawArchive.getArchiveStatus();
    rawArchive.getArchiveMetadata().setStatus(ArchiveStatus.INPROPERLY_EJECTED);
    rawArchive.getArchiveMetadata().setLogicalSpace(0L);
    rawArchive.updateLogicalFreeSpaceToMetaData();

    rawArchive.getArchiveMetadata().setUpdatedTime(System.currentTimeMillis());

    try {
      this.freeArchiveWithCheck(rawArchive);
    } catch (AllSegmentUnitDeadInArchviveException e) {
      logger.debug("all segment unit have dead");
    } catch (SegmentUnitBecomeBrokenException e) {
      logger.warn("set archive {} to deleting error", rawArchive, e);
    }
    ArchiveIdFileRecorder.IMPROPERLY_EJECTED_RAW
        .add(rawArchive.getArchiveMetadata().getArchiveId());

    for (ArchiveStatusChangeFinishListener listener : archiveStatusReportListers) {
      listener.becomeInProperlyEjected(rawArchive, oldArchiveStatus);
    }
  }

  @Override
  public void becomeEjected(Archive archive) throws ArchiveNotExistException {
    RawArchive rawArchive = this.getRawArchive(archive.getArchiveMetadata().getArchiveId());
    if (rawArchive == null) {
      throw new ArchiveNotExistException("archive: " + archive + "does not exist!");
    }
    final ArchiveStatus oldArchiveStatus = rawArchive.getArchiveStatus();

    logger.warn("an offlined disk has been plugged out, archive is {}", rawArchive);
    rawArchive.getArchiveMetadata().setStatus(ArchiveStatus.EJECTED);
    try {
      this.freeArchiveWithCheck(rawArchive);
    } catch (AllSegmentUnitDeadInArchviveException e) {
      logger.debug("all segment unit have dead");
    } catch (SegmentUnitBecomeBrokenException e) {
      logger.warn("set archive {} to deleting error", rawArchive, e);
    }

    rawArchive.getArchiveMetadata().setUpdatedTime(System.currentTimeMillis());
    rawArchive.updateLogicalFreeSpaceToMetaData();
    for (ArchiveStatusChangeFinishListener listener : archiveStatusReportListers) {
      listener.becomeEjected(rawArchive, oldArchiveStatus);
    }

  }

  @Override
  public void becomeBroken(Archive archive) throws ArchiveNotExistException {
    RawArchive rawArchive = this.getRawArchive(archive.getArchiveMetadata().getArchiveId());
    if (rawArchive == null) {
      throw new ArchiveNotExistException("archive: " + archive + "does not exist!");
    }
    final ArchiveStatus oldArchiveStatus = rawArchive.getArchiveStatus();

    boolean allSegmentUnitHasDeleted = false;
    try {
      this.freeArchiveWithCheck(rawArchive);
    } catch (AllSegmentUnitDeadInArchviveException e) {
      logger.debug("all segment unit have dead");
      allSegmentUnitHasDeleted = true;
    } catch (SegmentUnitBecomeBrokenException e) {
      logger.warn("set archive {} to deleting error", rawArchive, e);
    }

    if (!allSegmentUnitHasDeleted) {
      logger.warn("there has segmentUnit not deleted, wait and retry");
      diskErrorLogManager.rawArchiveBroken(rawArchive);
      return;
    }
    rawArchive.getArchiveMetadata().setStatus(ArchiveStatus.BROKEN);
    rawArchive.getArchiveMetadata().setLogicalSpace(0L);

    rawArchive.getStorage().setBroken(true);
    rawArchive.close();
    logger.warn("Have set archive {} to broken status", archive);

    for (ArchiveStatusChangeFinishListener listener : archiveStatusReportListers) {
      listener.becomeBroken(rawArchive, oldArchiveStatus);
    }
  }

  @Override
  public void becomeDegrade(Archive archive) throws ArchiveNotExistException {
    RawArchive rawArchive = this.getRawArchive(archive.getArchiveMetadata().getArchiveId());
    if (rawArchive == null) {
      throw new ArchiveNotExistException("archive: " + archive + "does not exist!");
    }
    final ArchiveStatus oldArchiveStatus = rawArchive.getArchiveStatus();

    rawArchive.getArchiveMetadata().setStatus(ArchiveStatus.DEGRADED);
    rawArchive.updateLogicalFreeSpaceToMetaData();
    logger.warn("set archive {} to degrade status", rawArchive);
    for (ArchiveStatusChangeFinishListener listener : archiveStatusReportListers) {
      listener.becomeDegrade(rawArchive, oldArchiveStatus);
    }
  }

  private boolean isArchiveMatchConfig(RawArchiveMetadata archiveMetadata) {
    if (archiveMetadata.getSegmentUnitSize() != dataNodeCfg.getSegmentUnitSize()) {
      logger.warn("archive{} segment size not match the data node config: {}", archiveMetadata,
          dataNodeCfg.getSegmentUnitSize());
      return false;
    }

    if (this.group != null && archiveMetadata.getGroup() != null) {
      if (!archiveMetadata.getGroup().equals(this.group)) {
        logger.warn("archive's group {} not match the group: {}", archiveMetadata.getGroup(),
            this.group);
        return false;
      }
    }

    Validate.notNull(this.instanceId);
    if (archiveMetadata.getInstanceId() != null) {
      if (!archiveMetadata.getInstanceId().equals(this.instanceId)) {
        logger.warn("archive's instanced {} not match the instance: {}",
            archiveMetadata.getInstanceId(), this.instanceId);
        return false;
      }
    }

    if (archiveMetadata.getPageSize() != dataNodeCfg.getPageSize()) {
      logger.warn("archive page size not match the pageSize: archive is {}, page size {}",
          archiveMetadata,
          dataNodeCfg.getPageSize());
      return false;
    }

    if (archiveMetadata.getFlexiableLimit() != dataNodeCfg.getFlexibleCountLimitInOneArchive()) {
      logger.error("config mismatch, due to different arbiter limit, archive metadata={}",
          archiveMetadata);
      return false;
    }

    return true;
  }

  private void correctConfigMismatchedArchive(RawArchive archive)
      throws StorageException, JsonProcessingException {
    Validate.isTrue(archive.getArchiveStatus() == ArchiveStatus.CONFIG_MISMATCH);
    archive.getArchiveMetadata().setSegmentUnitSize(dataNodeCfg.getSegmentUnitSize());
    archive.getArchiveMetadata().setPageSize(dataNodeCfg.getPageSize());
    archive.getArchiveMetadata().setFlexiableLimit(dataNodeCfg.getFlexibleCountLimitInOneArchive());

    archive.setGroup(group);
    archive.setInstanceId(instanceId);

    SegmentUnitMetadataAccessor
        .cleanAllSegmentMetadata(archive.getArchiveMetadata().getMaxSegUnitCount(),
            archive.getStorage());

    archive.updateArchiveConfiguration();

    RawArchiveBuilder
        .initArchiveMetadataWithStorageSizeNewVersion(archive.getArchiveMetadata().getArchiveType(),
            archive.getStorage().size(), archive.getArchiveMetadata());

    archive.persistMetadata();
    RawArchivePreAllocator preAllocator = new RawArchivePreAllocator(archive,
        dataNodeCfg, preAllocateBrickExecutor);
    preAllocateBrickExecutor.submit(preAllocator);

    archive.setCallback(unitTaskCallBack);
    archive.addSegmentUnitsToCatchupLogEngine(catchupLogEngine, false);
    archive.addSegmentUnitsToStateProcessingEngine(stateProcessingEngine, false);

    try {
      addToSegmentUnitManager(archive);
    } catch (Exception e) {
      logger.warn("caught an exception when add segment unit", e);
    }

    for (SegmentUnit segUnit : archive.getSegmentUnits()) {
      try {
        this.addSegmentUnitToEngines(segUnit);
      } catch (Exception ex) {
        logger.error("catch an exception when add segment unit {} to engines", segUnit, ex);
      }
    }

    int ratio = dataNodeCfg.getPrimaryFastBufferPercentage() + dataNodeCfg
        .getSecondaryFastBufferPercentage();
    long memorySize = dataNodeCfg.getMemorySizeForDataLogsMb();
    FastBufferManagerProxy fastBufferManagerProxy = new FastBufferManagerProxy(
        new PrimaryTlsfFastBufferManager(dataNodeCfg.getFastBufferAlignmentBytes(),
            memorySize * ratio / 100 * 1024L * 1024L),
        Storage.getDeviceName(archive.getStorage().identifier()));
    MutationLogEntryFactory.addFastBufferManager(archive.getArchiveId(), fastBufferManagerProxy);

  }

  @Override
  public void setMutationLogManager(MutationLogManager mutationLogManager) {
    this.mutationLogManager = mutationLogManager;
  }

  @Override
  public void setLogPersister(LogPersister logPersister) {
    this.logPersister = logPersister;
  }

  /**
   * For test purpose, set the archive's configuration different from system's.
   */
  @Override
  public void setArchiveConfig(long archiveId, SetArchiveConfigRequest request)
      throws JsonProcessingException, StorageException {
    RawArchive archive = this.getRawArchive(archiveId);

    RawArchiveMetadata archiveMetadata = archive.getArchiveMetadata();
    if (request.getGroupId() != 0) {
      archiveMetadata.setGroup(new Group(request.getGroupId()));
    }

    if (request.getInstanceId() != 0L) {
      archiveMetadata.setInstanceId(new InstanceId(request.getInstanceId()));
    }

    if (request.getPageSize() != 0) {
      archiveMetadata.setPageSize(request.getPageSize());
    }

    if (request.getSegmentSize() != 0L) {
      archiveMetadata.setSegmentUnitSize(request.getSegmentSize());
    }

    archive.persistMetadata();
  }

  @Override
  public void freeArchiveWithCheck(RawArchive archive)
      throws AllSegmentUnitDeadInArchviveException, SegmentUnitBecomeBrokenException {
    freeArchive(archive, true);
  }

  @Override
  public void freeArchiveWithOutCheck(RawArchive archive)
      throws AllSegmentUnitDeadInArchviveException, SegmentUnitBecomeBrokenException {
    freeArchive(archive, false);
  }

  public void freeArchive(RawArchive archive, boolean withCheck)
      throws AllSegmentUnitDeadInArchviveException, SegmentUnitBecomeBrokenException {
    Collection<SegmentUnit> segUnitList = archive.getSegmentUnits();

    boolean allSegmentUnitDead = true;

    for (SegmentUnit segUnit : segUnitList) {
      segUnit.lockStatus();
      try {
        if (segUnit.getSegmentUnitMetadata().getStatus() == SegmentUnitStatus.Deleted) {
          continue;
        }

        allSegmentUnitDead = false;
        SegId segId = segUnit.getSegId();
        if (segUnit.getSegmentUnitMetadata().getStatus() == SegmentUnitStatus.Broken) {
          continue;
        }

        if (withCheck) {
          segmentUnitCanDeletingCheck.deleteSegmentUnitWithCheck(segId);
        } else {
          try {
            segmentUnitCanDeletingCheck.deleteSegmentUnitWithOutCheck(segId, true);
          } catch (Exception e) {
            logger.error("can't update segment {} to Broken status", segUnit, e);
          }
        }
        logger.warn("set segment unit={} to broken,the id is{}",
            segmentUnitManager.get(segId), segId);

      } finally {
        segUnit.unlockStatus();
      }
    }

    if (allSegmentUnitDead) {
      logger.warn("all segment unit dead in archive{}", archive);
      throw new AllSegmentUnitDeadInArchviveException();
    }
  }

  @Override
  public void addSegmentUnitToEngines(SegmentUnit segUnit) throws Exception {
    Validate.isTrue(stateProcessingEngine != null, "stateProcessingEngine is null");
    SegId segId = segUnit.getSegId();
    if (!segUnit.isArbiter()) {
      Validate.isTrue(this.mutationLogManager != null, "mutationLogManager is null");
      Validate.isTrue(this.logPersister != null, "logPersister is null");
      Validate.isTrue(catchupLogEngine != null, "catchupLogEngine is null");
      Validate.isTrue(mutationLogManager.getSegment(segUnit.getSegId()) == null);
      SegmentLogMetadata logMetadata = mutationLogManager.createOrGetSegmentLogMetadata(segId,
          segUnit.getSegmentUnitMetadata().getVolumeType().getVotingQuorumSize());
      logMetadata.setSegmentUnit(segUnit);
      segUnit.setSegmentLogMetadata(logMetadata);

      logPersister.removeLogMetaData(segId);

      try {
        logPersister.persistLog(segId, MutationLogEntry.INVALID_MUTATION_LOG);
      } catch (Exception e) {
        logger.error("caught an exception when persist log: {}", segId);
      }

      catchupLogEngine.addSegmentUnit(segId, true);
    }

    stateProcessingEngine.addSegmentUnit(segId);
  }

  @Override
  public void recycleSegmentUnitFromDeleting(SegId segId)
      throws SegmentUnitRecoverFromDeletingFailException {
    RawArchive archive = segmentUnitManager.getArchive(segId);
    Validate.notNull(archive);
    archive.recycleSegmentFromDeleting(segId);
  }

  private void addArchive(Long archiveId, RawArchive archive) {
    this.mapArchiveIdToArchive.put(archiveId, archive);
    archive.updateLogicalFreeSpaceToMetaData();
    archive.setHostHeartbeat(this.heartbeat);
    if (!dataNodeCfg.getArchiveConfiguration().isOnVms() && dataNodeCfg.isStartSlowDiskChecker()) {
      Storage storage = archive.getStorage();
      if (storage instanceof PriorityStorageImpl) {
        ((PriorityStorageImpl) storage)
            .checkSlowDisk((int) dataNodeCfg.getSlowDiskLatencyThresholdSequentialNs(),
                (int) dataNodeCfg.getSlowDiskLatencyThresholdRandomNs(),
                dataNodeCfg.getSlowDiskIgnoreRatio(), dataNodeCfg.getSlowDiskStrategy(),
                storage);
      }
    }
  }

  @Override
  public void recoverBitmap(LogPersistingConfiguration dataNodeLogPersistingCfg) {
    List<Thread> threadSet = new ArrayList<>();
    for (RawArchive archive : mapArchiveIdToArchive.values()) {
      Thread thread = new Thread(() -> {
        logger.warn("recover bitmap for segment unit in archive {}", archive);
        try {
          archive.recoverBitmapForSegmentUnit(dataNodeLogPersistingCfg);
        } catch (Throwable se) {
          logger.error("catch an exception when recovering shadow pages, disk broken", se);
          try {
            archive.setArchiveStatus(ArchiveStatus.BROKEN);
          } catch (Exception e) {
            logger.error("cannot off set archive broken after recover bit map falied !", e);
          }
        }

      }
      );

      thread.start();
      threadSet.add(thread);
    }

    for (Thread thread : threadSet) {
      do {
        try {
          thread.join();
        } catch (InterruptedException e) {
          logger.error("catch an exception when join recover bitmap", e);
          continue;
        }
        break;
      } while (true);
    }
  }

  public void setStoragePool(RawArchive archive, Long storagePoolId)
      throws JsonProcessingException, StorageException {
    if (archive.getArchiveStatus() != ArchiveStatus.CONFIG_MISMATCH) {
      if (storagePoolId == null) {
        logger
            .warn("please watch storagePoolId set to null, in archive {}", archive.getArchiveId());
      }
      archive.setStoragePoolId(storagePoolId);
      archive.persistMetadata();
    }
  }

  @Override
  public void plugin(Archive archive)
      throws ArchiveTypeNotSupportException, InvalidInputException, ArchiveStatusException,
      ArchiveIsNotCleanedException {
    logger.warn("some one plug in the archive {}", archive);
    Validate.isTrue(archive.getArchiveMetadata().getArchiveType() == ArchiveType.RAW_DISK);
    RawArchive newArchive = (RawArchive) archive;
    newArchive.setSyncLogTaskExecutor(this.syncLogTaskExecutor);
    newArchive.getSegmentUnits().forEach(segmentUnit -> {
      if (segmentUnit.getSecondaryPclDriver() == null) {
        SecondaryPclDriver pclDriver = new SecondaryPclDriver(segmentUnit,
            syncLogTaskExecutor.getConfiguration(), syncLogTaskExecutor);
        segmentUnit.setSecondaryPclDriver(pclDriver);
      }
    });

    RawArchive currentArchive = getRawArchive(newArchive.getArchiveId());
    if (currentArchive != null) {
      isAllSegmentUnitDeleted(currentArchive);
    }

    newArchive.setCallback(unitTaskCallBack);

    RawArchivePreAllocator preAllocator = new RawArchivePreAllocator(newArchive,
        dataNodeCfg, preAllocateBrickExecutor);
    preAllocateBrickExecutor.submit(preAllocator);
    cleanUpPeriodExecutor.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        if (newArchive.canBeUsed()) {
          newArchive.cleanUpTimeoutLogs(
              dataNodeCfg.getTimeoutOfCleanUpCrashOutUuidMs(),
              dataNodeCfg.getTimeoutOfCleanUpCompletingLogsMs());
        }
      }
    }, 0, dataNodeCfg.getPeriodCleanUpTimeoutLogsPerArchive(), TimeUnit.MILLISECONDS);

    newArchive.clearArchiveStatusListener();
    newArchive.addListener(this);

    boolean newArchiveBroken = false;
    if (diskErrorLogManager != null) {
      try {
        diskErrorLogManager.putArchive(newArchive);
      } catch (DiskBrokenException e) {
        logger.error(
            "RawArchive has more than the threshold of storage exceptions. " 
                + "Set it status to Broken. archive: {}",
            newArchive.getArchiveId(), e);
        newArchiveBroken = true;
      } catch (DiskDegradeException e) {
        logger.warn("the disk has IO exception, archive {}", newArchive, e);
      }
    }

    if (currentArchive != null) {
      logger.warn("i found the archive {} from memory ", currentArchive);
      ArchiveStatus oldStatus = currentArchive.getArchiveStatus();
      Validate.isTrue(
          oldStatus == ArchiveStatus.EJECTED || oldStatus == ArchiveStatus.INPROPERLY_EJECTED);

      try {
        currentArchive.getStorage().close();
      } catch (StorageException e) {
        logger.warn("close the archive {}is error", currentArchive);
      }
      if (currentArchive.getStorage().identifier()
          .equalsIgnoreCase(newArchive.getStorage().identifier())) {
        logger.warn("archiveWithSameSerialNum  {} with the same storage", currentArchive);
      } else {
        logger.warn("archiveWithSameSerialNum {} with the  diffent storage", currentArchive);
        AsynchronousFileChannelStorageFactory.getInstance().close(currentArchive.getStorage());
      }

      mapArchiveIdToArchive.remove(currentArchive.getArchiveId());
    }

    Archive archiveBaseWithSameSerialNum = this
        .getArchiveBySerialNumber(newArchive.getArchiveMetadata().getSerialNumber());
    RawArchive archiveWithSameSerialNum = (RawArchive) archiveBaseWithSameSerialNum;
    if (archiveWithSameSerialNum != null) {
      logger.warn(
          "found an old archive with serial number same as the new one, old archive {}, " 
              + "new archive {}",
          archiveWithSameSerialNum, newArchive);
      ArchiveStatus oldStatus = archiveWithSameSerialNum.getArchiveStatus();
      Validate.isTrue(
          oldStatus == ArchiveStatus.EJECTED || oldStatus == ArchiveStatus.INPROPERLY_EJECTED);

      boolean needRemoveThreadPoolBehindStorage = !archiveWithSameSerialNum.getStorage()
          .identifier()
          .equals(newArchive.getStorage().identifier());

      boolean allArchiveSegmentUnitDeleted = false;
      try {
        freeArchiveWithCheck(archiveWithSameSerialNum);
      } catch (AllSegmentUnitDeadInArchviveException e) {
        logger.warn("remove the segment unit {}", archiveWithSameSerialNum);
        allArchiveSegmentUnitDeleted = true;
        removeArchive(archiveWithSameSerialNum.getArchiveId(), needRemoveThreadPoolBehindStorage);
        try {
          archiveWithSameSerialNum.getStorage().close();
          if (archiveWithSameSerialNum.getStorage().identifier()
              .equalsIgnoreCase(newArchive.getStorage().identifier())) {
            logger.warn("archiveWithSameSerialNum  {} with the same storage",
                archiveWithSameSerialNum);
          } else {
            logger.warn("archiveWithSameSerialNum {} with the  diffent storage",
                archiveWithSameSerialNum);
            AsynchronousFileChannelStorageFactory.getInstance()
                .close(archiveWithSameSerialNum.getStorage());
          }
        } catch (StorageException se) {
          logger.warn("caught an exception", e);
        }
      } catch (SegmentUnitBecomeBrokenException e) {
        logger.warn("mark the segment unit error", e);
      }
      if (!allArchiveSegmentUnitDeleted) {
        throw new ArchiveIsNotCleanedException();
      }
    }

    addArchive(newArchive.getArchiveId(), newArchive);

    ArchiveStatus newStatus = null;
    ArchiveStatus statusFromDisk = newArchive.getArchiveStatus();
    if (newArchiveBroken || statusFromDisk == ArchiveStatus.BROKEN) {
      newStatus = ArchiveStatus.BROKEN;
    } else if (!isArchiveMatchConfig(newArchive.getArchiveMetadata())
        || statusFromDisk == ArchiveStatus.CONFIG_MISMATCH) {
      logger.warn("the newly insert disk's configuration not match, archive {}", newArchive);
      newStatus = ArchiveStatus.CONFIG_MISMATCH;
    } else if (statusFromDisk == ArchiveStatus.OFFLINING || statusFromDisk == ArchiveStatus.OFFLINED
        || statusFromDisk == ArchiveStatus.GOOD) {
      newStatus = ArchiveStatus.GOOD;
    } else if (statusFromDisk == ArchiveStatus.DEGRADED) {
      newStatus = ArchiveStatus.DEGRADED;
    } else {
      throw new ArchiveStatusException("");
    }

    if (statusFromDisk == ArchiveStatus.OFFLINING) {
      try {
        newArchive.setArchiveStatus(ArchiveStatus.INPROPERLY_EJECTED);
      } catch (Exception ex) {
        logger.error("catch an exception when set archive status, archive {}, new status {}",
            newArchive,
            newStatus, ex);
        return;
      }
    }
    try {
      newArchive.setArchiveStatus(newStatus);
    } catch (Exception ex) {
      logger.error("catch an exception when set archive status, archive {}, new status {}",
          newArchive, newStatus,
          ex);
      return;
    }

    if (newArchive.getArchiveStatus() != ArchiveStatus.BROKEN
        && newArchive.getArchiveStatus() != ArchiveStatus.CONFIG_MISMATCH) {
      try {
        addToSegmentUnitManager(newArchive);
      } catch (Exception e) {
        logger.warn("caught an exception when add segment unit", e);
      }

      newArchive.setCatchupLogEngine(catchupLogEngine);
      newArchive.setStateProcessingEngine(stateProcessingEngine);
      for (SegmentUnit segUnit : newArchive.getSegmentUnits()) {
        try {
          this.addSegmentUnitToEngines(segUnit);
        } catch (Exception ex) {
          logger.error("catch an exception when add segment unit {} to engines", segUnit, ex);
        }
      }
    }

    int ratio = dataNodeCfg.getPrimaryFastBufferPercentage() + dataNodeCfg
        .getSecondaryFastBufferPercentage();
    long memorySize = dataNodeCfg.getMemorySizeForDataLogsMb();
    FastBufferManagerProxy fastBufferManagerProxy = new FastBufferManagerProxy(
        new PrimaryTlsfFastBufferManager(dataNodeCfg.getFastBufferAlignmentBytes(),
            memorySize * ratio / 100 * 1024L * 1024L),
        Storage.getDeviceName(newArchive.getStorage().identifier()));
    MutationLogEntryFactory.addFastBufferManager(newArchive.getArchiveId(), fastBufferManagerProxy);
  }

  @Override
  public Archive plugout(String devName, String serialNumber)
      throws ArchiveTypeNotSupportException, ArchiveNotFoundException {
    RawArchive plugout = null;
    logger.warn("some one plug out the archive ,devName is {},serialNumber is {}", devName,
        serialNumber);
    plugout = getArchiveBySerialNumber(serialNumber);

    if (plugout == null) {
      logger
          .warn("i can not found the archive, devName={}, serialNumber={}", devName, serialNumber);
      throw new ArchiveNotFoundException(
          "can not find the archive=" + devName + ", serial=" + serialNumber);
    }

    logger.warn("I have found the raw archive {} to plug out", plugout);
    ArchiveStatus currentStatus = plugout.getArchiveStatus();
    ArchiveStatus newStatus = (currentStatus == ArchiveStatus.OFFLINED
        || currentStatus == ArchiveStatus.CONFIG_MISMATCH) 
        ? ArchiveStatus.EJECTED :
        ArchiveStatus.INPROPERLY_EJECTED;
    try {
      logger.warn("set archive with new status {}, archive is {}", newStatus, plugout);
      currentStatus.validate(newStatus);
      switch (newStatus) {
        case EJECTED:
          plugout.setArchiveStatus(ArchiveStatus.EJECTED);
          MutationLogEntryFactory.deleteFastBufferManager(plugout.getArchiveId());
          break;
        case INPROPERLY_EJECTED:
          plugout.setArchiveStatus(ArchiveStatus.INPROPERLY_EJECTED);
          MutationLogEntryFactory.deleteFastBufferManager(plugout.getArchiveId());
          break;
        default:
          logger.warn("plug out the archvie {}status {}is exception", plugout, currentStatus);
          Validate.isTrue(false);
      }

    } catch (Exception e) {
      logger
          .error("catch an exception when set archive {} to new status {}", plugout, newStatus, e);
    }

    logger.warn("set archive {} to new status {}", plugout, newStatus);
    return plugout;
  }

  @Override
  public void hasPlugoutFinished(Archive archive) throws ArchiveIsNotCleanedException {
    isAllSegmentUnitDeleted((RawArchive) archive);
  }

  @Override
  public List<Archive> getArchives() {
    List<Archive> archives = new ArrayList<>();
    for (Map.Entry<Long, RawArchive> entry : mapArchiveIdToArchive.entrySet()) {
      archives.add(entry.getValue());
    }
    return archives;
  }

  @Override
  public Archive getArchive(long archiveId) {
    return mapArchiveIdToArchive.get(archiveId);
  }

  private void isAllSegmentUnitDeleted(RawArchive archive) throws ArchiveIsNotCleanedException {
    List<SegId> removeSegUnitId = new ArrayList<>();
    for (SegmentUnit segmentUnit : archive.getSegmentUnits()) {
      SegmentUnit segUnit = segmentUnitManager.get(segmentUnit.getSegId());
      if (segUnit != null) {
        if (!segUnit.getArchive().getArchiveId().equals(archive.getArchiveId())) {
          logger.warn("the segmentunit {} is may be in other archive {}", segmentUnit,
              segUnit.getArchive());
          removeSegUnitId.add(segUnit.getSegId());
          continue;
        } else {
          logger.warn("the segment unit {} is not be delete from manager", segUnit);
          throw new ArchiveIsNotCleanedException();
        }
      }

    }

    for (SegId segId : removeSegUnitId) {
      try {
        archive.removeSegmentUnit(segId);
      } catch (Exception e) {
        logger.warn("remove segment unit {} error", segId, e);
      }
    }

    return;
  }

  public void setArbiterManager(ArbiterManager arbiterManager) {
    this.arbiterManager = arbiterManager;
  }

  public void countLogs() {
    logger.warn("couting logs");
    for (RawArchive rawArchive : mapArchiveIdToArchive.values()) {
      rawArchive.countLogs();
    }
  }

  class RawArchivePreAllocator implements Delayed, Runnable {
    private RawArchive rawArchive;
    private DataNodeConfiguration dataNodeConfiguration;
    private ConsumerService<RawArchivePreAllocator> consumerService;
    private long delayMs;
    private long origin;
    private long interval;
    private boolean isArbitArchivePreAllocator;

    public RawArchivePreAllocator(RawArchive rawArchive,
        DataNodeConfiguration dataNodeConfiguration,
        ConsumerService<RawArchivePreAllocator> consumerService) {
      this(rawArchive, dataNodeConfiguration, consumerService, 1000, false);
    }

    public RawArchivePreAllocator(RawArchive rawArchive,
        DataNodeConfiguration dataNodeConfiguration,
        ConsumerService<RawArchivePreAllocator> consumerService,
        boolean isArbitArchivePreAllocator) {
      this(rawArchive, dataNodeConfiguration, consumerService, 1000, isArbitArchivePreAllocator);
    }

    public RawArchivePreAllocator(RawArchive rawArchive,
        DataNodeConfiguration dataNodeConfiguration,
        ConsumerService<RawArchivePreAllocator> consumerService, long delayMs,
        boolean isArbitArchivePreAllocator) {
      this.rawArchive = rawArchive;
      this.dataNodeConfiguration = dataNodeConfiguration;
      this.interval = delayMs;
      this.consumerService = consumerService;
      this.isArbitArchivePreAllocator = isArbitArchivePreAllocator;
      updateDelay(delayMs);
    }

    public void updateDelay(long newDelayMs) {
      long now = System.currentTimeMillis();
      if (now + newDelayMs >= delayMs + origin) {
        delayMs = newDelayMs;
        origin = now;
      }
    }

    @Override
    public void run() {
      try {
        logger.info("begin prepare allocate metadata {}", rawArchive.getArchiveMetadata());
        if (!shutdownFlag.get()) {
          logger.info("sechdule one prepare allocate metadata {}", rawArchive.getArchiveMetadata());
          if (rawArchive.preAllocateBrick(
              dataNodeConfiguration.getBrickPreAllocatorLowerThreshold(),
              dataNodeConfiguration.getBrickPreAllocatorUpperThreshold(),
              isArbitArchivePreAllocator)) {
            updateDelay(interval);
            consumerService.submit(this);
          } else {
            logger.error("sechdule failed prepare allocate metadata {}",
                rawArchive.getArchiveMetadata());
          }
        }
      } catch (Throwable e) {
        logger.error("catch some exception", e);
      }
    }

    @Override
    public long getDelay(TimeUnit unit) {
      return unit.convert(origin + delayMs - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public final int compareTo(Delayed delayed) {
      logger.trace("this={}, delayed={}", this, delayed);
      if (delayed == null) {
        return 1;
      }

      if (delayed == this) {
        return 0;
      }

      long d = (getDelay(TimeUnit.MILLISECONDS) - delayed.getDelay(TimeUnit.MILLISECONDS));
      return ((d == 0) ? 0 : ((d < 0) ? -1 : 1));
    }
  }
}
