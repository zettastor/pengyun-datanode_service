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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.AbstractArchiveBuilder;
import py.archive.Archive;
import py.archive.ArchiveMetadata;
import py.archive.ArchiveOptions;
import py.archive.ArchiveStatus;
import py.archive.ArchiveType;
import py.archive.RawArchiveMetadata;
import py.archive.brick.BrickMetadata;
import py.archive.brick.BrickStatus;
import py.archive.segment.SegId;
import py.archive.segment.SegmentUnitBitmap;
import py.archive.segment.SegmentUnitMetadata;
import py.archive.segment.SegmentUnitStatus;
import py.archive.segment.SegmentUnitType;
import py.archive.segment.recurring.SegmentUnitTaskCallback;
import py.common.RequestIdBuilder;
import py.common.bitmap.Bitmap;
import py.common.tlsf.bytebuffer.manager.TlsfByteBufferManager;
import py.common.tlsf.bytebuffer.manager.TlsfByteBufferManagerFactory;
import py.datanode.ArchiveBuilderCallback;
import py.datanode.configuration.DataNodeConfiguration;
import py.datanode.segment.SegmentUnitMetadataAccessor;
import py.datanode.segment.datalog.sync.log.SyncLogTaskExecutor;
import py.exception.ArchiveTypeMismatchException;
import py.exception.ChecksumMismatchedException;
import py.exception.StorageException;
import py.storage.Storage;

public class RawArchiveBuilder extends AbstractArchiveBuilder {
  private static final Logger logger = LoggerFactory.getLogger(RawArchiveBuilder.class);
  private static final int MAX_NONE_MAGIC_COUNT = 100;
  private SegmentUnitTaskCallback segmentUnitTaskCallback;
  private ArchiveBuilderCallback archiveBuilderCallback;
  private DataNodeConfiguration cfg;
  private SyncLogTaskExecutor syncLogTaskExecutor;

  public RawArchiveBuilder(DataNodeConfiguration cfg, Storage storage) {
    this(cfg, storage, true);
  }

  public RawArchiveBuilder(DataNodeConfiguration cfg, Storage storage,
      boolean justloadingExistArchive) {
    super(ArchiveType.RAW_DISK, storage);
    this.justloadingExistArchive = justloadingExistArchive;
    this.cfg = cfg;
  }

  public static void initArchiveMetadataWithStorageSizeNewVersion(ArchiveType archiveType,
      long storageSize,
      RawArchiveMetadata archiveMetadata) {
    long singleSpaceForSegment = ArchiveOptions.SEGMENT_PHYSICAL_SIZE;

    long segmentUnitMetadataOffset = archiveType.getArchiveHeaderLength();
    long fixedHeadLength = segmentUnitMetadataOffset + ArchiveOptions.ALL_FLEXIBLE_LENGTH;

    if (fixedHeadLength > storageSize) {
      logger.error(
          "fixed head length :{}, dynamic space length {} great than storage size",
          fixedHeadLength, ArchiveOptions.ALL_FLEXIBLE_LENGTH);
      throw new RuntimeException("the archive: space is too little");
    }

    long spacePerBrick =
        ArchiveOptions.BRICK_DESCDATA_LENGTH + ArchiveOptions.SEGMENT_PHYSICAL_SIZE;
    long maxSegmentUnit = (storageSize - fixedHeadLength) / spacePerBrick;

    if (maxSegmentUnit <= 0) {
      logger.error(
          "space too small. fixed head length :{} . singleSpaceForSegment:{}, storage size {}",
          fixedHeadLength, spacePerBrick, storageSize);
      throw new RuntimeException("the archive: space is too little");
    }

    long brickMetadataOffset = fixedHeadLength;
    long segmentUnitDataOffset =
        brickMetadataOffset + (ArchiveOptions.BRICK_DESCDATA_LENGTH * maxSegmentUnit);

    int alignment = ArchiveOptions.SEGMENT_UNIT_DATA_ALIGNED_SIZE;
    segmentUnitDataOffset = (segmentUnitDataOffset / alignment + 1) * alignment;
    long recalculationSegmentUnitCount =
        (storageSize - segmentUnitDataOffset) / ArchiveOptions.SEGMENT_PHYSICAL_SIZE;
    if (recalculationSegmentUnitCount != maxSegmentUnit) {
      maxSegmentUnit -= 1;
    }

    if (maxSegmentUnit <= 0) {
      logger.error(
          "space too small. fixed head length :{} . singleSpaceForSegment:{}, storage size {}",
          fixedHeadLength, spacePerBrick, storageSize);
      throw new RuntimeException("the archive: space is too little");
    }

    archiveMetadata.setMaxSegUnitCount((int) maxSegmentUnit);

    segmentUnitDataOffset =
        brickMetadataOffset + (ArchiveOptions.BRICK_DESCDATA_LENGTH * maxSegmentUnit);
    segmentUnitDataOffset = (segmentUnitDataOffset / alignment + 1) * alignment;

    long virtualSegmentUnitStartOffset = segmentUnitDataOffset 
        + ((storageSize - segmentUnitDataOffset)
            / ArchiveOptions.SEGMENT_PHYSICAL_SIZE + 1) * ArchiveOptions.SEGMENT_PHYSICAL_SIZE;

    archiveMetadata.setSegmentUnitDataOffset(segmentUnitDataOffset);
    archiveMetadata.setSegmentUnitMetadataOffset(segmentUnitMetadataOffset);
    archiveMetadata.setBrickMetadataOffset(brickMetadataOffset);
    archiveMetadata.setVirtualSegmentUnitDataOffset(virtualSegmentUnitStartOffset);

    logger.info(
        "storage size {}, max segment count {}, Desc offset {}, segUnit data offset {}",
        storageSize, maxSegmentUnit, segmentUnitMetadataOffset, segmentUnitDataOffset);
  }

  @Override
  public Archive build()
      throws StorageException, IOException, ChecksumMismatchedException, Exception {
    RawArchiveMetadata archiveMetadataFromStorage = null;
    try {
      archiveMetadataFromStorage = (RawArchiveMetadata) loadArchiveMetadata();
    } catch (ArchiveTypeMismatchException e) {
      boolean iserror = true;
      if ((firstTimeStart && overwrite) || forceInitBuild) {
        iserror = false;
      }
      if (iserror) {
        logger.warn(" load archive  catch the error ", e);
        throw e;
      }
    }

    if (archiveMetadataFromStorage == null && justloadingExistArchive) {
      throw new RuntimeException("the storage=" + storage + " is not formatted");
    }

    ArchiveOptions
        .initContants(cfg.getPageSize(), cfg.getSegmentUnitSize(), cfg.getPageMetadataSize(),
            cfg.getArchiveReservedRatio(), cfg.getFlexibleCountLimitInOneArchive(),
            cfg.isPageMetadataNeedFlushToDisk());

    boolean archiveIsBlack = archiveMetadataFromStorage != null
        && ArchiveIdFileRecorder.IMPROPERLY_EJECTED_RAW
        .contains(archiveMetadataFromStorage.getArchiveId());

    RawArchive rawArchive;

    if (archiveMetadataFromStorage == null
        || (firstTimeStart && overwrite)) {
      Validate.isTrue(!justloadingExistArchive);
      RawArchiveMetadata oldMetadata = archiveMetadataFromStorage;
      logger.warn("formatting old archive metadata={}", archiveMetadataFromStorage);

      archiveMetadataFromStorage = new RawArchiveMetadata(generateArchiveMetadata());

      archiveMetadataFromStorage.setStorageType(storageType);

      archiveMetadataFromStorage.setStatus(ArchiveStatus.GOOD);
      initArchiveMetadataWithStorageSizeNewVersion(archiveMetadataFromStorage.getArchiveType(),
          storage.size(),
          archiveMetadataFromStorage);
      archiveMetadataFromStorage.setSegmentUnitSize(cfg.getSegmentUnitSize());

      archiveMetadataFromStorage.setCreatedTime(System.currentTimeMillis());
      archiveMetadataFromStorage.setCreatedBy(currentUser);
      archiveMetadataFromStorage.setPageSize(cfg.getPageSize());
      archiveMetadataFromStorage.setSegmentUnitSize(cfg.getSegmentUnitSize());

      archiveMetadataFromStorage.setFlexiableLimit(cfg.getFlexibleCountLimitInOneArchive());
      logger.debug("overwrite:{}, inRealTimeRunning:{}, on matter what clearing the all metadata",
          overwrite,
          runInRealTime);

      SegmentUnitMetadataAccessor
          .cleanAllSegmentMetadata(archiveMetadataFromStorage.getMaxSegUnitCount(), storage);
      Long newArchiveId = RequestIdBuilder.get();
      logger.warn("set the archive id {} to the archive {}", newArchiveId,
          archiveMetadataFromStorage);
      archiveMetadataFromStorage.setArchiveId(newArchiveId);
      logger.warn("formatting new archive metadata={}", archiveMetadataFromStorage);
      archiveMetadataFromStorage.setStatus(ArchiveStatus.GOOD);
      rawArchive = buildNewNormalRawArchive(storage, archiveMetadataFromStorage);

      if (archiveIsBlack) {
        ArchiveIdFileRecorder.IMPROPERLY_EJECTED_RAW.remove(oldMetadata.getArchiveId());
      }
    } else {
      logger.warn("load a existing archive={}", archiveMetadataFromStorage);
      if (archiveBuilderCallback != null && !archiveBuilderCallback
          .configMatched(archiveMetadataFromStorage)) {
        logger
            .error("archive 's configuration does not match current data node 's configuration. {}",
                archiveMetadataFromStorage);
        return new RawArchive(archiveMetadataFromStorage, Lists.newArrayList(),
            Lists.newArrayList(),
            storage, segmentUnitTaskCallback, syncLogTaskExecutor);
      }

      if (archiveIsBlack) {
        logger.warn(
            "this disk={} has been plugged out by force and in black, deleting all segment units",
            storage);
        SegmentUnitMetadataAccessor
            .cleanAllSegmentMetadata(archiveMetadataFromStorage.getMaxSegUnitCount(), storage);
        ArchiveIdFileRecorder.IMPROPERLY_EJECTED_RAW
            .remove(archiveMetadataFromStorage.getArchiveId());
      }

      if (justloadingExistArchive) {
        return buildExistingRawArchive(storage, archiveMetadataFromStorage);
      }

      archiveMetadataFromStorage.setUpdatedTime(System.currentTimeMillis());
      archiveMetadataFromStorage.setUpdatedBy(currentUser);
      if (!archiveMetadataFromStorage.getSerialNumber().equals(serialNumber)) {
        logger.warn(
            "serial number is not equal: disk serial number %s, serial number from command %s",
            archiveMetadataFromStorage.getSerialNumber(), serialNumber);

        if (archiveMetadataFromStorage.getDeviceName()
            .equalsIgnoreCase(archiveMetadataFromStorage.getSerialNumber())) {
          archiveMetadataFromStorage.setSerialNumber(serialNumber);
        } else {
          Validate.isTrue(false,
              "serial number is not equal: disk serial number %s, serial number from command %s",
              archiveMetadataFromStorage.getSerialNumber(), serialNumber);
        }
      }
      archiveMetadataFromStorage.setDeviceName(devName);
      archiveMetadataFromStorage.setFileSystemPartitionName(fileSystemPartitionName);
      rawArchive = buildNewNormalRawArchive(storage, archiveMetadataFromStorage);
    }

    logger
        .warn("write archiveMeta to disk: {} to storage: {}", archiveMetadataFromStorage, storage);
    rawArchive.persistMetadata();

    return rawArchive;
  }

  @Override
  public ArchiveMetadata instantiate(byte[] buffer, int offset, int length) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    RawArchiveMetadata archiveMetadata = mapper
        .readValue(buffer, offset, length, RawArchiveMetadata.class);
    return archiveMetadata;
  }

  public RawArchive reLoadArbiterArchive()
      throws StorageException, IOException, ChecksumMismatchedException, Exception {
    RawArchiveMetadata archiveMetadataFromStorage = null;
    try {
      archiveMetadataFromStorage = (RawArchiveMetadata) loadArchiveMetadata();
    } catch (ArchiveTypeMismatchException e) {
      boolean iserror = true;
      if ((firstTimeStart && overwrite) || forceInitBuild) {
        iserror = false;
      }
      if (iserror) {
        logger.warn(" load archive  catch the error ", e);
        throw e;
      }
    }
    return initArbiterArchive(archiveMetadataFromStorage);
  }

  public RawArchive initArbiterArchive(RawArchiveMetadata rawArchiveMetadata)
      throws IOException, StorageException {
    RawArchive rawArchive;

    if (rawArchiveMetadata == null) {
      rawArchiveMetadata = new RawArchiveMetadata(generateArchiveMetadata());

      rawArchiveMetadata.setStorageType(storageType);

      rawArchiveMetadata.setStatus(ArchiveStatus.GOOD);

      rawArchiveMetadata.setSegmentUnitSize(cfg.getSegmentUnitSize());

      rawArchiveMetadata.setCreatedTime(System.currentTimeMillis());
      rawArchiveMetadata.setCreatedBy(currentUser);
      rawArchiveMetadata.setPageSize(cfg.getPageSize());
      rawArchiveMetadata.setSegmentUnitSize(cfg.getSegmentUnitSize());
      rawArchive = buildNewArbiterRawArchive(storage, rawArchiveMetadata);
    } else {
      rawArchive = buildExistingArbiterRawArchive(storage, rawArchiveMetadata);
    }
    rawArchive.persistMetadata();
    return rawArchive;

  }

  public RawArchiveBuilder setSegmentUnitTaskCallback(
      SegmentUnitTaskCallback segmentUnitTaskCallback) {
    this.segmentUnitTaskCallback = segmentUnitTaskCallback;
    return this;
  }

  public RawArchiveBuilder setArchiveBuilderCallback(
      ArchiveBuilderCallback archiveBuilderCallback) {
    this.archiveBuilderCallback = archiveBuilderCallback;
    return this;
  }

  public RawArchive buildExistingRawArchive(Storage storage, RawArchiveMetadata archiveMetadata)
      throws IOException, StorageException {
    if (archiveMetadata.getArchiveId() == 0) {
      logger.error(
          "No archive id on an existing archive={}, please initialize the archive first, " 
              + "archive metadata={}",
          storage, archiveMetadata);

      throw new RuntimeException("the id of the archive=" + storage + " is not right");
    }

    List<SegmentUnitMetadata> segUnitMetadataList = new ArrayList<>();
    List<BrickMetadata> brickMetadataList = new ArrayList<>();

    if (archiveMetadata.getStatus() == ArchiveStatus.GOOD
        || archiveMetadata.getStatus() == ArchiveStatus.DEGRADED
        || archiveMetadata.getStatus() == ArchiveStatus.OFFLINED) {
      readSegmentAndBrickFromStorage(storage, segUnitMetadataList, brickMetadataList);
      logger.warn("Get segment units {} from the archive, brick list {}", segUnitMetadataList,
          brickMetadataList);
    }

    return new RawArchive(archiveMetadata, segUnitMetadataList, brickMetadataList,
        storage, segmentUnitTaskCallback, syncLogTaskExecutor);
  }

  private RawArchive buildNewRawArchive(Storage storage, RawArchiveMetadata archiveMetadata,
      boolean isArbiter)
      throws IOException, StorageException {
    logger.debug("overwrite is {}, inRealTimeRunning {}", overwrite, runInRealTime);

    List<SegmentUnitMetadata> segUnitMetadataList = new ArrayList<>();
    List<BrickMetadata> brickMetadataList = new ArrayList<>();

    readSegmentAndBrickFromStorage(storage, segUnitMetadataList, brickMetadataList);

    if (isArbiter) {
      Validate.isTrue(0 == brickMetadataList.size());
    }
    RawArchive archive = new RawArchive(archiveMetadata, segUnitMetadataList,
        brickMetadataList, storage, segmentUnitTaskCallback, isArbiter, syncLogTaskExecutor);

    logger.warn("segment units from disk are {}, brick read from disk is {}", segUnitMetadataList,
        brickMetadataList);

    return archive;
  }

  private RawArchive buildNewNormalRawArchive(Storage storage, RawArchiveMetadata archiveMetadata)
      throws IOException, StorageException {
    logger.debug("overwrite is {}, inRealTimeRunning {}", overwrite, runInRealTime);

    return buildNewRawArchive(storage, archiveMetadata, false);
  }

  private RawArchive buildNewArbiterRawArchive(Storage storage, RawArchiveMetadata archiveMetadata)
      throws IOException, StorageException {
    logger.debug("overwrite is {}, inRealTimeRunning {}", overwrite, runInRealTime);

    return buildNewRawArchive(storage, archiveMetadata, true);
  }

  private RawArchive buildExistingArbiterRawArchive(Storage storage,
      RawArchiveMetadata archiveMetadata)
      throws IOException, StorageException {
    List<SegmentUnitMetadata> segUnitMetadataList = new ArrayList<>();
    List<BrickMetadata> brickMetadataList = new ArrayList<>();

    readSegmentAndBrickFromStorage(storage, segUnitMetadataList, brickMetadataList);

    Validate.isTrue(0 == brickMetadataList.size());
    return new RawArchive(archiveMetadata, segUnitMetadataList, brickMetadataList,
        storage, segmentUnitTaskCallback, true, syncLogTaskExecutor);
  }

  private void readSegmentAndBrickFromStorage(Storage storage,
      List<SegmentUnitMetadata> segUnitMetadataList, List<BrickMetadata> brickMetadataList)
      throws IOException, StorageException {
    Validate.isTrue(segUnitMetadataList != null && brickMetadataList != null);
    logger.warn("readSegmentAndShadowUnitFromStorage, from {}",
        ArchiveOptions.SEGMENTUNIT_DESCDATA_REGION_OFFSET);
    readSegmentUnitsFromStorage(storage, segUnitMetadataList,
        ArchiveOptions.SEGMENTUNIT_DESCDATA_REGION_OFFSET,
        storage.size());

    readBricksFromStorage(storage, brickMetadataList,
        ArchiveOptions.SEGMENTUNIT_DESCDATA_REGION_OFFSET + ArchiveOptions.ALL_FLEXIBLE_LENGTH,
        storage.size());

    Map<SegId, SegmentUnitMetadata> segIdSegmentUnitMetadataMap = new HashMap<>();
    segUnitMetadataList.forEach(segmentUnitMetadata -> {
      logger.debug("put segment unit to map, segment unit {}", segmentUnitMetadata);
      if (!segmentUnitMetadata.getStatus().equals(SegmentUnitStatus.Deleted)) {
        Validate.isTrue(segIdSegmentUnitMetadataMap
            .put(segmentUnitMetadata.getSegId(), segmentUnitMetadata) == null);
      } else {
        logger.warn("found a deleted segment unit, no need set brick metadata. seg id {}",
            segmentUnitMetadata.getSegId());
      }
    });

    brickMetadataList.forEach(brickMetadata -> {
      SegmentUnitMetadata segmentUnitMetadata = segIdSegmentUnitMetadataMap
          .get(brickMetadata.getSegId());
      logger.debug("get segment unit from map {}, segment unit {}", brickMetadata,
          segmentUnitMetadata);
      if (segmentUnitMetadata != null) {
        segmentUnitMetadata.setBrickMetadata(brickMetadata);
      }
    });

    Validate.isTrue(
        segUnitMetadataList.stream().allMatch(segmentUnitMetadata -> {
          if (segmentUnitMetadata.getSegmentUnitType().equals(SegmentUnitType.Normal)
              && !segmentUnitMetadata.getStatus().equals(SegmentUnitStatus.Deleted)) {
            if (segmentUnitMetadata.getBrickMetadata() == null) {
              logger.error(
                  "after load segment unit metadata and brick. " 
                      + "found a normal segment can not match brick {}",
                  segmentUnitMetadata);
              if (segmentUnitMetadata.getStatus().equals(SegmentUnitStatus.Deleting)) {
                logger.warn(
                    "deleting segment unit maybe has free brick metadata. so ignore this validate");
                return true;
              }
            }
            return segmentUnitMetadata.getBrickMetadata() != null;
          } else {
            return true;
          }
        }));

  }

  private void readSegmentUnitsFromStorage(Storage storage,
      List<SegmentUnitMetadata> segmentUnitMetadataList,
      long readStartPosition, long readEndPosition)
      throws StorageException, IOException {
    TlsfByteBufferManager tlsfByteBufferManager = TlsfByteBufferManagerFactory.instance();
    Validate.notNull(tlsfByteBufferManager);

    ByteBuffer buffer = tlsfByteBufferManager
        .blockingAllocate(ArchiveOptions.SEGMENTUNIT_METADATA_LENGTH);

    try {
      int nonMagicCount = 0;
      while (true) {
        if (readStartPosition >= readEndPosition) {
          logger.warn("come to the end of read position, segmentUnitMetadataList is {}",
              segmentUnitMetadataList);
          return;
        }
        storage.read(readStartPosition, buffer);
        buffer.clear();
        long magic = buffer.getLong();

        if (magic != ArchiveOptions.SEGMENT_UNIT_MAGIC) {
          if (++nonMagicCount > MAX_NONE_MAGIC_COUNT) {
            return;
          }
        } else if (nonMagicCount > 0) {
          logger.error("!!FATAL!!, some segment units almost missed ! {} {} {}", nonMagicCount,
              storage, readStartPosition);
          try {
            Thread.sleep(5000);
          } catch (InterruptedException ignore) {
            logger.error("", ignore);
          }
          System.exit(-1);
        }

        buffer.clear();

        if (magic == ArchiveOptions.SEGMENT_UNIT_MAGIC) {
          SegmentUnitMetadata segmentUnitMetadata = SegmentUnitMetadataAccessor
              .readSegmentUnitMetadataFromBuffer(buffer);
          segmentUnitMetadata.setMetadataOffsetInArchive(readStartPosition);
          segmentUnitMetadata.setStorage(storage);
          SegmentUnitBitmap bitmap = ArchiveUnitBitMapAccessor
              .readBitMapFromDisk(segmentUnitMetadata);
          segmentUnitMetadata.setBitMap(bitmap);
          segmentUnitMetadataList.add(segmentUnitMetadata);

          if (!segmentUnitMetadata.getStatus().isFinalStatus()) {
            logger.warn("read a segment unit {}", segmentUnitMetadata);
          }

          if (segmentUnitMetadata.getStatus() == SegmentUnitStatus.Deleted) {
            segmentUnitMetadata.setIsPersistDeletedStatus(true);
          }
        }

        readStartPosition += ArchiveOptions.SEGMENTUNIT_DESCDATA_LENGTH;
        buffer.clear();
      }
    } finally {
      tlsfByteBufferManager.release(buffer);
    }
  }

  private void readBricksFromStorage(Storage storage, List<BrickMetadata> brickMetadataList,
      long readStartPosition, long readEndPosition)
      throws StorageException, IOException {
    TlsfByteBufferManager tlsfByteBufferManager = TlsfByteBufferManagerFactory.instance();
    Validate.notNull(tlsfByteBufferManager);

    ByteBuffer buffer = tlsfByteBufferManager
        .blockingAllocate(ArchiveOptions.BRICK_METADATA_LENGTH);

    try {
      int nonMagicCount = 0;
      while (true) {
        if (readStartPosition >= readEndPosition) {
          logger.warn("come to the end of read position, segmentUnitMetadataList is {}",
              brickMetadataList);
          return;
        }
        storage.read(readStartPosition, buffer);
        buffer.clear();
        long magic = buffer.getLong();

        if (magic != ArchiveOptions.BRICK_MAGIC) {
          if (++nonMagicCount > MAX_NONE_MAGIC_COUNT) {
            return;
          }
        } else if (nonMagicCount > 0) {
          logger.error("!!FATAL!!, some brick almost missed ! {} {} {}", nonMagicCount, storage,
              readStartPosition);
          try {
            Thread.sleep(5000);
          } catch (InterruptedException ignore) {
            logger.error("", ignore);
          }
          System.exit(-1);
        }

        buffer.clear();

        if (magic == ArchiveOptions.BRICK_MAGIC) {
          BrickMetadata brickMetadata = SegmentUnitMetadataAccessor
              .readBrickMetadataFromBuffer(buffer);
          brickMetadata.setStorage(storage);
          Bitmap bitmap = ArchiveUnitBitMapAccessor
              .readBrickAllocatedBitMapFromDisk(brickMetadata);
          brickMetadata.setAllocateBitmap(bitmap);
          BrickSpaceManagerImpl brickSpaceManager = new BrickSpaceManagerImpl(brickMetadata);
          brickMetadata.setBrickSpaceManager(brickSpaceManager);
          brickMetadataList.add(brickMetadata);
          if (brickMetadata.getStatus().equals(BrickStatus.free)) {
            logger.warn("read a brick metadata {}", brickMetadata);
          }
        }

        readStartPosition += ArchiveOptions.BRICK_DESCDATA_LENGTH;
        buffer.clear();
      }
    } finally {
      tlsfByteBufferManager.release(buffer);
    }
  }

  public void setSyncLogTaskExecutor(
      SyncLogTaskExecutor syncLogTaskExecutor) {
    this.syncLogTaskExecutor = syncLogTaskExecutor;
  }
}
