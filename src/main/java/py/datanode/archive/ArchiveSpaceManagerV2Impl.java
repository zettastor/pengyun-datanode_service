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

import com.google.common.annotations.VisibleForTesting;
import java.nio.channels.CompletionHandler;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.AbstractSegmentUnitMetadata;
import py.archive.ArchiveOptions;
import py.archive.RawArchiveMetadata;
import py.archive.brick.BrickMetadata;
import py.archive.brick.BrickSpaceManager;
import py.archive.brick.BrickStatus;
import py.archive.segment.SegId;
import py.archive.segment.SegmentUnitBitmap.SegmentUnitBitMapType;
import py.archive.segment.SegmentUnitMetadata;
import py.archive.segment.SegmentUnitStatus;
import py.archive.segment.SegmentUnitType;
import py.archive.segment.recurring.MigratingSegmentUnitMetadata;
import py.common.bitmap.Bitmap;
import py.datanode.exception.CannotAllocMoreFlexibleException;
import py.datanode.segment.PersistDataToDiskEngineImpl;
import py.datanode.segment.SegmentUnit;
import py.exception.NotEnoughSpaceException;
import py.exception.StorageException;
import py.instance.InstanceId;
import py.membership.SegmentMembership;
import py.volume.VolumeType;

public class ArchiveSpaceManagerV2Impl implements ArchiveSpaceManager {
  private static final Logger logger = LoggerFactory.getLogger(ArchiveSpaceManagerV2Impl.class);

  private final RawArchive archive;

  private final Set<BrickMetadata> bricksWithFreeSpace;

  private final long virtualSegmentUnitStartOffset;

  private final Map<Long, AbstractSegmentUnitMetadata> mapOffsetToArchiveUnit = new TreeMap<>();

  private final Map<Long, BrickMetadata> mapOffsetToBrick = new TreeMap<>();

  private final Map<Long, BrickMetadata> waitingPersistCleanDirtyFlag = new TreeMap<>();

  private final TreeMap<Long, BrickMetadata> preAllocateBrickMap = new TreeMap<>();

  private final TreeSet<BrickMetadata> canReusedBrickMetadata;
  private final TreeSet<AbstractSegmentUnitMetadata> canReusedUnitMetadata;
  private final BitSet unitMetadataAllocateBitSet;
  private final BitSet brickMetadataAllocateBitSet;

  public ArchiveSpaceManagerV2Impl(RawArchive archive,
      List<SegmentUnitMetadata> segUnitMetadataList,
      List<BrickMetadata> brickMetadataList) {
    this.archive = archive;

    canReusedBrickMetadata = new TreeSet<>(Comparator.comparingLong(BrickMetadata::getDataOffset));
    canReusedUnitMetadata = new TreeSet<>(
        Comparator.comparingLong(AbstractSegmentUnitMetadata::getLogicalDataOffset));
    unitMetadataAllocateBitSet = new BitSet(ArchiveOptions.MAX_FLEXIBLE_COUNT);
    brickMetadataAllocateBitSet = new BitSet(archive.getArchiveMetadata().getMaxSegUnitCount());

    for (SegmentUnitMetadata segUnit : segUnitMetadataList) {
      recordUnitMetadataAllocated(segUnit);
    }

    bricksWithFreeSpace = new TreeSet<>(Comparator.comparingLong(BrickMetadata::getDataOffset));
    for (BrickMetadata brickMetadata : brickMetadataList) {
      recordBrickMetadataAllocated(brickMetadata);

      if (brickMetadata.isShadowUnit() && brickMetadata.getFreePageCount() != 0) {
        bricksWithFreeSpace.add(brickMetadata);
      }
    }

    this.virtualSegmentUnitStartOffset = archive.getArchiveMetadata()
        .getVirtualSegmentUnitDataOffset();
  }

  public SegmentUnitMetadata allocateSegmentUnit(SegId segId, SegmentMembership membership,
      VolumeType volumeType, String volumeMetadataJson, String accountMetadataJson,
      SegmentUnitType segmentUnitType,
      long srcVolumeId,  SegmentMembership srcMembership,
      boolean isSecondaryCandidate, InstanceId replace)
      throws NotEnoughSpaceException, CannotAllocMoreFlexibleException {
    AbstractSegmentUnitMetadata canbeReusableUnit = this
        .lookForOneReusableNormalUnitFromPreProcessingQueue();
    SegmentUnitMetadata segMetadata = null;
    if (canbeReusableUnit != null) {
      segMetadata = new SegmentUnitMetadata(segId, canbeReusableUnit.getLogicalDataOffset(),
          membership, SegmentUnitStatus.Start, canbeReusableUnit.getMetadataOffsetInArchive(),
          archive.getStorage(), volumeType, segmentUnitType, volumeMetadataJson,
          accountMetadataJson, srcVolumeId,  srcMembership,
          isSecondaryCandidate, replace);

      recordUnitMetadataAllocated(segMetadata);
    } else {
      if (!adequateForNewArchiveUnit()) {
        logger.warn("space not enough, archive will become overloaded, {}",
            archive.getArchiveMetadata());
      }
      try {
        long dataOffset = this.lookForFreeSpaceForNewUnit();
        if (-1 != dataOffset) {
          long metadataOffset = this.getMetadataOffset(dataOffset);
          segMetadata = new SegmentUnitMetadata(segId, dataOffset, membership,
              SegmentUnitStatus.Start, metadataOffset, archive.getStorage(), volumeType,
              segmentUnitType,  volumeMetadataJson, accountMetadataJson, srcVolumeId,
              srcMembership,  isSecondaryCandidate, replace);

          recordUnitMetadataAllocated(segMetadata);
          archive.updateLogicalFreeSpaceToMetaData();
        }
      } catch (CannotAllocMoreFlexibleException e) {
        canbeReusableUnit = this.lookForOneReusableNormalUnit();
        if (canbeReusableUnit != null) {
          segMetadata = new SegmentUnitMetadata(segId, canbeReusableUnit.getLogicalDataOffset(),
              membership, SegmentUnitStatus.Start, canbeReusableUnit.getMetadataOffsetInArchive(),
              archive.getStorage(), volumeType, segmentUnitType,  volumeMetadataJson,
              accountMetadataJson, srcVolumeId,  srcMembership,
              isSecondaryCandidate, replace);

          recordUnitMetadataAllocated(segMetadata);
        }
      }
    }

    if (segMetadata == null) {
      logger.warn("after look reusable and new unit, can not found any free unit for new unit");
      throw new CannotAllocMoreFlexibleException();
    }
    logger.warn("allocate a segment unit metadata space begin at {}, segment metadata {}",
        segMetadata.getLogicalDataOffset(), segMetadata);

    if (segmentUnitType != SegmentUnitType.Arbiter && segmentUnitType != SegmentUnitType.Flexible) {
     
      BrickMetadata reusableBrick = null;
      reusableBrick = this.lookForOneReusableBrickFromPreProcessingQueue(true);

      try {
        if (reusableBrick != null) {
          reusableBrick.clear();
          reusableBrick.setStatus(BrickStatus.allocated);
          segMetadata.setBrickMetadata(reusableBrick);
          archive.updateLogicalFreeSpaceToMetaData();

          logger.warn("reused a brick space begin at {}, segment metadata {}",
              reusableBrick.getDataOffset(), reusableBrick);
        } else {
         
          try {
            long brickOffset = this
                .lookForFreeSpaceForNewBrick();
            long metadataOffset = this.getBrickMetadataByDataOffset(brickOffset);
            logger.warn("found an unit, offset {}, metadataOffset {}. start unit offset {}",
                brickOffset, metadataOffset,
                archive.getArchiveMetadata().getSegmentUnitDataOffset());
            SegId physicalSegId = new SegId(archive.getArchiveMetadata().getArchiveId(),
                (int) brickOffset);
            BrickMetadata newBrickMetadata = new BrickMetadata(physicalSegId, brickOffset,
                metadataOffset,
                BrickStatus.allocated, archive.getStorage(), ArchiveOptions.PAGE_NUMBER_PER_SEGMENT,
                ArchiveOptions.PAGE_NUMBER_PER_SEGMENT);
            BrickSpaceManager brickSpaceManager = new BrickSpaceManagerImpl(newBrickMetadata);
            newBrickMetadata.setBrickSpaceManager(brickSpaceManager);
            segMetadata.setBrickMetadata(newBrickMetadata);
            recordBrickMetadataAllocated(newBrickMetadata);
            archive.updateLogicalFreeSpaceToMetaData();

            logger.warn("allocate a brick space begin at {}, segment metadata {}",
                newBrickMetadata.getDataOffset(), newBrickMetadata);
          } catch (NotEnoughSpaceException e) {
            logger.warn(
                "can not found any more free bricks," 
                    + " try allocate brick in pre-allocated brick map");
            reusableBrick = this.lookForOneReusableBrick(false);
            if (reusableBrick != null) {
              preAllocateBrickMap.remove(reusableBrick.getDataOffset());
              reusableBrick.setIsPreAllocated(false);
              reusableBrick.clear();
              reusableBrick.setStatus(BrickStatus.allocated);
              segMetadata.setBrickMetadata(reusableBrick);
              archive.updateLogicalFreeSpaceToMetaData();

              logger.warn("reused a brick space begin at {}, segment metadata {}",
                  reusableBrick.getDataOffset(), reusableBrick);
            } else {
              throw new NotEnoughSpaceException();
            }
          }
        }
      } catch (NotEnoughSpaceException e) {
        segMetadata.setStatus(SegmentUnitStatus.Deleted);
        logger.warn("catch an not enough space exception when allocate an brick", e);
        throw e;
      } catch (Exception e) {
        logger.warn("catch an exception when allocate an brick ", e);
      }

    }

    return segMetadata;
  }

  @Deprecated
  public void replaceAbstractSegmentUnitMetadata(AbstractSegmentUnitMetadata from,
      AbstractSegmentUnitMetadata to) {
  }

  @Deprecated
  public MigratingSegmentUnitMetadata allocateMigratingSegmentUnit(SegId sourceSegId)
      throws StorageException, NotEnoughSpaceException {
    throw new NotImplementedException("");
  }

  @Deprecated
  public boolean freeAbstractSegmentUnitMetadata(
      AbstractSegmentUnitMetadata abstractSegmentUnitMetadata) {
    throw new NotImplementedException("");
  }

  public long getFreeBrickSpace() {
    return getFreeSpaceWithoutReservedSpace();
  }

  public long getTotalSpace() {
    return getTotalSpaceWithoutReservedSpace();
  }

  public long getUsedSpace() {
    long usedPageCount = 0L;
    List<BrickMetadata> brickMetadataList = new ArrayList<>();

    for (BrickMetadata brickMetadata : mapOffsetToBrick.values()) {
      if (brickMetadata.getStatus() != BrickStatus.shadowPageAllocated) {
        continue;
      }
      brickMetadataList.add(brickMetadata);
    }
    for (SegmentUnit segmentUnit : archive.getSegmentUnits()) {
      if (segmentUnit.getSegmentUnitMetadata().getStatus().isFinalStatus()
          || segmentUnit.getSegmentUnitMetadata().getSegmentUnitType() != SegmentUnitType.Normal) {
        continue;
      }
      BrickMetadata brickMetadata = segmentUnit.getSegmentUnitMetadata()
          .getBrickMetadata();
      switch (brickMetadata.getStatus()) {
        case allocated:
          usedPageCount +=
              segmentUnit.getSegmentUnitMetadata().getPageCount() - segmentUnit
                  .getSegmentUnitMetadata().getFreePageCount();
          break;
        case shadowPageAllocated:
          Bitmap unitBitMap = Bitmap.valueOf(brickMetadata.getAllocateBitmap().toByteArray());
          unitBitMap.and(segmentUnit.getSegmentUnitMetadata().getBitmap().getBitMap(
              SegmentUnitBitMapType.Data));
          usedPageCount += unitBitMap.cardinality();
          brickMetadataList.remove(brickMetadata);
          break;
        default:
      }

    }

    for (BrickMetadata brickMetadata : brickMetadataList) {
      Validate.isTrue(brickMetadata.getStatus() == BrickStatus.shadowPageAllocated);
      usedPageCount += brickMetadata.getUsedPageCount();
    }
    return usedPageCount * ArchiveOptions.PAGE_SIZE;
  }

  /**
   * If new segment could be created on this archive.
   */
  public boolean acceptNewSegmentUnitCreation() {
   
   
   
    return adequateForNewArchiveUnit();
  }

  public long getLargestContiguousFreeSpaceLogically() {
    long largest = getLargestContiguousFreeSpacePhysically();
    return (long) ((int) (largest / ArchiveOptions.SEGMENT_PHYSICAL_SIZE))
        * ArchiveOptions.SEGMENT_SIZE;
  }

  /**
   * Persist the bit map to disk.
   */
  public void persistBitMap() {
    for (AbstractSegmentUnitMetadata archiveUnit : mapOffsetToArchiveUnit.values()) {
      if (archiveUnit.isBitmapNeedPersisted()) {
        try {
          ArchiveUnitBitMapAccessor.writeBitMapToDisk(archiveUnit);
          archiveUnit.setBitmapNeedPersisted(false);
        } catch (Exception e) {
          logger.error("persist bitmap fail", e);
        }
      }
    }

    for (BrickMetadata brickMetadata : mapOffsetToBrick.values()) {
      if (brickMetadata.isBitmapNeedPersisted()) {
        try {
          ArchiveUnitBitMapAccessor.writeBrickBitMapToDisk(brickMetadata);
          brickMetadata.setBitmapNeedPersisted(false);
        } catch (Exception e) {
          logger.error("persist bitmap fail", e);
        }
      }
    }
  }

  public int getArbiterInUsedCount() {
    int count = 0;
    count = getSegmentUnitInUserdCount(mapOffsetToArchiveUnit, SegmentUnitType.Arbiter);
    return count;
  }

  public int getFlexibleInUsedCount() {
    int count = 0;
    count = getSegmentUnitInUserdCount(mapOffsetToArchiveUnit, SegmentUnitType.Flexible);
    return count;
  }

  public int getNormalInUsedCount() {
    int count = 0;
    count = getSegmentUnitInUserdCount(mapOffsetToArchiveUnit, SegmentUnitType.Normal);
    return count;
  }

  public int getSegmentUnitInUsedCount() {
    int count = 0;
    for (Entry<Long, AbstractSegmentUnitMetadata> entry : mapOffsetToArchiveUnit.entrySet()) {
      AbstractSegmentUnitMetadata archiveUnit = entry.getValue();
      if (!archiveUnit.isUnitReusable()) {
        count++;
      }
    }
    logger.debug("in used segment unit count is {}", count);
    return count;

  }

  public boolean isFlexibleLogicalAddress(long dataOffsetInArchive) {
    return dataOffsetInArchive >= virtualSegmentUnitStartOffset;
  }

  public Collection<BrickMetadata> getAllBrickMetadata() {
    return new ArrayList<>(mapOffsetToBrick.values());
  }

  public BrickMetadata getArchiveBrickByOffset(long offset) {
    return mapOffsetToBrick.get(offset);
  }

  public void persistCleanDirtyFlag() {
    for (BrickMetadata brickMetadata : waitingPersistCleanDirtyFlag.values()) {
      try {
        persistBrickMetadata(brickMetadata);
      } catch (Exception e) {
        logger.error("catch an exception when persist brick metadata", e);
      }
    }
    waitingPersistCleanDirtyFlag.clear();
  }

  @VisibleForTesting
  public AbstractSegmentUnitMetadata getArchiveUnitByOffset(long offset) {
    return mapOffsetToArchiveUnit.get(offset);
  }

  @VisibleForTesting
  public List<BrickMetadata> getShadowUnitList() {
    List<BrickMetadata> shadowUnitList = new ArrayList<>();
    for (BrickMetadata brickMetadata : mapOffsetToBrick.values()) {
      if (brickMetadata.isShadowUnit()) {
        shadowUnitList.add(brickMetadata);
      }
    }
    return shadowUnitList;
  }

  public boolean markBrickInGarbageCollection(BrickMetadata brickMetadata, boolean marked) {
    return brickMetadata.setMarkedBrickCannotAllocatePages(marked);
  }

  public void preAllocateBrick(int lowerThreshold, int upperThreshold,
      boolean isArbitPreAllocator) {
    preProcessCanReusableMetadata();
    if (!isArbitPreAllocator) {
      if (preAllocateBrickMap.size() < lowerThreshold) {
        int count = upperThreshold - preAllocateBrickMap.size();
        while (count-- > 0) {
          try {
            allocateOneBrickAndInsertToPreBrickMap();
          } catch (NotEnoughSpaceException e) {
            logger.info("can not pre allocate brick", e);
            return;
          }
        }
      }
    }

    return;
  }

  @Override
  public long getBrickStartOffset(long physicalOffsetOnArchive) {
    long startOffset = archive.getArchiveMetadata().getSegmentUnitDataOffset();
    return startOffset
        + (physicalOffsetOnArchive - startOffset) / ArchiveOptions.SEGMENT_PHYSICAL_SIZE
        * ArchiveOptions.SEGMENT_PHYSICAL_SIZE;
  }

  private AbstractSegmentUnitMetadata lookForOneReusableNormalUnit() {
   
    return lookForOneReusableUnit(mapOffsetToArchiveUnit);
  }

  private AbstractSegmentUnitMetadata lookForOneReusableNormalUnitFromPreProcessingQueue() {
    AbstractSegmentUnitMetadata unitMetadata = null;
    while (canReusedUnitMetadata.size() > 0) {
      unitMetadata = canReusedUnitMetadata.pollFirst();
      if (unitCanBeReused(unitMetadata)) {
        return unitMetadata;
      }
    }

    return null;
  }

  private AbstractSegmentUnitMetadata lookForOneReusableUnit(
      Map<Long, AbstractSegmentUnitMetadata> segmentUnitMap) {
    for (Entry<Long, AbstractSegmentUnitMetadata> entry : segmentUnitMap.entrySet()) {
      AbstractSegmentUnitMetadata archiveUnit = entry.getValue();
      if (unitCanBeReused(archiveUnit)) {
        return archiveUnit;
      }
    }
    return null;
  }

  private long lookForFreeSpaceForNewUnit() throws CannotAllocMoreFlexibleException {
    int clearIndex = unitMetadataAllocateBitSet.nextClearBit(0);
    if (clearIndex < 0 || clearIndex >= ArchiveOptions.MAX_FLEXIBLE_COUNT) {
      logger.warn("unit metadata allocate bitset is full, can not allocate more segment");
      throw new CannotAllocMoreFlexibleException();
    }

    long dataOffset = archive.getSegmentUnitLogicalDataStartPosition()
        + clearIndex * ArchiveOptions.SEGMENT_PHYSICAL_SIZE;

    logger.info("find a new segment unit, data offset {}", dataOffset);
    return dataOffset;
  }

  private long lookForFreeSpaceForNewUnit(long size) throws CannotAllocMoreFlexibleException {
    long offset = archive.getArchiveMetadata().getVirtualSegmentUnitDataOffset();

    long contiguousFreeSpace = getContiguousFreeSpaceAfterForUnit(offset);
    logger.debug("allocate new segment unit continuous {}, offset {}", contiguousFreeSpace, offset);
    if (contiguousFreeSpace > size) {
      return offset;
    }

    for (AbstractSegmentUnitMetadata segmentUnitMetadata : mapOffsetToArchiveUnit.values()) {
      offset = endOfArchiveUnit(segmentUnitMetadata);
      contiguousFreeSpace = getContiguousFreeSpaceAfterForUnit(offset);
      if (contiguousFreeSpace < size) {
       
        continue;
      } else {
        logger.debug("find an area started at {} with free space {} more than {}", offset,
            contiguousFreeSpace,
            size);
        return offset;
      }
    }
    throw new CannotAllocMoreFlexibleException();
  }

  private long getContiguousFreeSpaceAfterForUnit(long offset) {
    long nextUnitOffset =
        this.virtualSegmentUnitStartOffset + (ArchiveOptions.MAX_FLEXIBLE_COUNT
            * ArchiveOptions.SEGMENT_PHYSICAL_SIZE);
    for (Entry<Long, AbstractSegmentUnitMetadata> entry : this.mapOffsetToArchiveUnit.entrySet()) {
      long unitOffset = entry.getValue().getLogicalDataOffset();
      if (unitOffset >= offset && unitOffset < nextUnitOffset) {
        nextUnitOffset = unitOffset;
      }
    }
    return nextUnitOffset - offset;
  }

  private long endOfArchiveUnit(AbstractSegmentUnitMetadata segmentUnitMetadata) {
    return segmentUnitMetadata.getLogicalDataOffset() + ArchiveOptions.SEGMENT_PHYSICAL_SIZE;
  }

  private long getMetadataOffset(long dataOffset) {
    Validate
        .isTrue((dataOffset - this.archive.getArchiveMetadata().getVirtualSegmentUnitDataOffset())
            % ArchiveOptions.SEGMENT_PHYSICAL_SIZE == 0);

    long index = (dataOffset - this.archive.getArchiveMetadata().getVirtualSegmentUnitDataOffset())
        / ArchiveOptions.SEGMENT_PHYSICAL_SIZE;

    long offset = ArchiveOptions.SEGMENTUNIT_DESCDATA_REGION_OFFSET
        + index * ArchiveOptions.SEGMENTUNIT_DESCDATA_LENGTH;

    logger.debug("offset is {}, SEGMENTUNIT_DESCDATA_REGION_OFFSET {}", offset,
        ArchiveOptions.SEGMENTUNIT_DESCDATA_REGION_OFFSET);

    Validate.isTrue(ArchiveOptions.SEGMENTUNIT_DESCDATA_REGION_OFFSET <= offset
        && offset + ArchiveOptions.SEGMENTUNIT_DESCDATA_LENGTH
        <= ArchiveOptions.BRICK_METADATA_REGION_OFFSET);

    return offset;
  }

  private BrickMetadata lookForOneReusableBrick(Map<Long, BrickMetadata> brickMetadataMap,
      boolean needCheckPreAllocated) {
    for (Entry<Long, BrickMetadata> entry : brickMetadataMap.entrySet()) {
      BrickMetadata brickMetadata = entry.getValue();
      if (brickCanBeReused(brickMetadata, needCheckPreAllocated)) {
        return brickMetadata;
      }
    }
    return null;
  }

  private BrickMetadata lookForOneReusableBrick(boolean needCheckPreAllocated) {
   
    return lookForOneReusableBrick(mapOffsetToBrick, needCheckPreAllocated);
  }

  private BrickMetadata lookForOneReusableBrickFromPreProcessingQueue(
      boolean needCheckPreAllocated) {
   
    BrickMetadata brickMetadata = null;
    while (canReusedBrickMetadata.size() > 0) {
      brickMetadata = canReusedBrickMetadata.pollFirst();
      if (brickCanBeReused(brickMetadata, needCheckPreAllocated)) {
        return brickMetadata;
      }
    }

    return null;
  }

  private long lookForFreeSpaceForNewBrick() throws NotEnoughSpaceException {
    int clearIndex = brickMetadataAllocateBitSet.nextClearBit(0);
    if (clearIndex < 0 || clearIndex >= archive.getArchiveMetadata().getMaxSegUnitCount()) {
      logger.warn("unit metadata allocate bitset is full, can not allocate more segment");
      throw new NotEnoughSpaceException();
    }

    long dataOffset = archive.getSegmentUnitDataStartPosition()
        + clearIndex * ArchiveOptions.SEGMENT_PHYSICAL_SIZE;

    logger.info("find a new brick, data offset {}", dataOffset);
    return dataOffset;

  }

  private long lookForFreeSpaceForNewBrick(long size) throws NotEnoughSpaceException {
    long offset = archive.getArchiveMetadata().getSegmentUnitDataOffset();

    long contiguousFreeSpace = getContiguousFreeSpaceAfterForBrick(offset);
    if (contiguousFreeSpace > size) {
      return offset;
    }

    for (BrickMetadata brickMetadata : mapOffsetToBrick.values()) {
      offset = endOfArchiveBrick(brickMetadata);
      contiguousFreeSpace = getContiguousFreeSpaceAfterForBrick(offset);
      if (contiguousFreeSpace < size) {
       
        continue;
      } else {
        logger.warn("find an area started at {} with free space {} more than {}", offset,
            contiguousFreeSpace,
            size);
        return offset;
      }
    }
    throw new NotEnoughSpaceException();
  }

  private long getContiguousFreeSpaceAfterForBrick(long offset) {
    long nextUnitOffset = archive.getStorage().size();
    for (Entry<Long, BrickMetadata> entry : this.mapOffsetToBrick.entrySet()) {
      long unitOffset = entry.getValue().getDataOffset();
      if (unitOffset >= offset && unitOffset < nextUnitOffset) {
        nextUnitOffset = unitOffset;
      }
    }
    return nextUnitOffset - offset;
  }

  private long endOfArchiveBrick(BrickMetadata brickMetadata) {
    return brickMetadata.getDataOffset() + ArchiveOptions.SEGMENT_PHYSICAL_SIZE;
  }

  private long getBrickMetadataByDataOffset(long dataOffset) {
    Validate.isTrue((dataOffset - this.archive.getArchiveMetadata().getSegmentUnitDataOffset())
        % ArchiveOptions.SEGMENT_PHYSICAL_SIZE == 0);

    long index = (dataOffset - this.archive.getArchiveMetadata().getSegmentUnitDataOffset())
        / ArchiveOptions.SEGMENT_PHYSICAL_SIZE;

    long metadataOffset = ArchiveOptions.BRICK_METADATA_REGION_OFFSET
        + index * (ArchiveOptions.BRICK_METADATA_LENGTH + ArchiveOptions.BRICK_BITMAP_LENGTH);

    logger.debug("metadata offset is {}, BRICK_METADATA_REGION_OFFSET {}", metadataOffset,
        ArchiveOptions.BRICK_METADATA_REGION_OFFSET);

    Validate.isTrue(ArchiveOptions.BRICK_METADATA_REGION_OFFSET <= metadataOffset
        && metadataOffset < archive.getSegmentUnitDataStartPosition());

    return metadataOffset;
  }

  private void persistBrickMetadata(BrickMetadata brickMetadata) throws Exception {
    recordBrickMetadataAllocated(brickMetadata);
    PersistDataToDiskEngineImpl.getInstance().writeBrickMetadata(brickMetadata);
  }

  private void persistBrickMetadata(BrickMetadata brickMetadata,
      CompletionHandler<Integer, BrickMetadata> callback) throws Exception {
    recordBrickMetadataAllocated(brickMetadata);
    PersistDataToDiskEngineImpl.getInstance().writeBrickMetadata(brickMetadata, callback);
  }

  private long getFreeSpaceWithoutReservedSpace() {
    long logicalUsedSpace = getLogicalUsedBrickSpace();
    long totalSpaceWithoutReservedSpace = getTotalSpaceWithoutReservedSpace();
    if (logicalUsedSpace > totalSpaceWithoutReservedSpace) {
      archive.setOverloaded(true);
      return 0L;
    } else {
      archive.setOverloaded(false);
      long segmentUnitSize = archive.getArchiveMetadata().getSegmentUnitSize();
      return ((totalSpaceWithoutReservedSpace - logicalUsedSpace) / segmentUnitSize)
          * segmentUnitSize;
    }
  }

  private long getLogicalUsedBrickSpace() {
    int countArchiveUnitInUsed = 0;
    for (BrickMetadata brickMetadata : mapOffsetToBrick.values()) {
      if (!brickMetadata.isUnitReusable()) {
        countArchiveUnitInUsed++;
      }
    }
    return countArchiveUnitInUsed * archive.getArchiveMetadata().getSegmentUnitSize();
  }

  private long getTotalSpaceWithoutReservedSpace() {
    RawArchiveMetadata archiveMetadata = archive.getArchiveMetadata();
    int maxSegUnitCount = archiveMetadata.getMaxSegUnitCount();
    int reservedSegUnitCount = (int) (maxSegUnitCount * ArchiveOptions.ARCHIVE_RESERVED_RATIO);
    return (maxSegUnitCount - reservedSegUnitCount) * archiveMetadata.getSegmentUnitSize();
  }

  private long getLargestContiguousFreeSpacePhysically() {
    long largest = getContiguousFreeSpaceAfterForBrick(
        this.archive.getArchiveMetadata().getSegmentUnitDataOffset());
    for (BrickMetadata brickMetadata : mapOffsetToBrick.values()) {
      largest = Math
          .max(largest, getContiguousFreeSpaceAfterForBrick(endOfArchiveBrick(brickMetadata)));
    }
    return largest;
  }

  private boolean adequateForNewArchiveUnit() {
    long freeSpace = getFreeSpaceWithoutReservedSpace();
    return freeSpace >= archive.getArchiveMetadata().getSegmentUnitSize();
  }

  private int getSegmentUnitInUserdCount(
      Map<Long, AbstractSegmentUnitMetadata> segmentUnitMetadataMap,
      SegmentUnitType segmentUnitType) {
    int count = 0;
    for (Entry<Long, AbstractSegmentUnitMetadata> entry : segmentUnitMetadataMap.entrySet()) {
      AbstractSegmentUnitMetadata archiveUnit = entry.getValue();
      if (!archiveUnit.isUnitReusable() && (archiveUnit.getSegmentUnitType() == segmentUnitType)) {
        count++;
      }
    }
    logger.debug("in used SegmentUnitType:{} count is {}", segmentUnitType, count);
    return count;
  }

  private void allocateOneBrickAndInsertToPreBrickMap() throws NotEnoughSpaceException {
    CompletionHandler<Integer, BrickMetadata> completionHandler = 
        new CompletionHandler<Integer, BrickMetadata>() {
          @Override
          public void completed(Integer result, BrickMetadata attachment) {
            if (attachment.isPreAllocated()) {
              preAllocateBrickMap.put(attachment.getDataOffset(), attachment);
            }
          }
    
          @Override
          public void failed(Throwable exc, BrickMetadata attachment) {
            attachment.setIsPreAllocated(false);
            logger.error("persist brick metadata {} failed", attachment, exc);
          }
        };

    BrickMetadata reusableBrick = null;
    reusableBrick = this.lookForOneReusableBrickFromPreProcessingQueue(true);
    if (reusableBrick != null) {
      try {
        reusableBrick.clear(true);

        reusableBrick.setIsPreAllocated(true);
        persistBrickMetadata(reusableBrick, completionHandler);
        return;
      } catch (Exception e) {
        logger.error("catch an exception when allocate an free page", e);
      }
    }

    try {
      long brickOffset = this.lookForFreeSpaceForNewBrick();
      long metadataOffset = this.getBrickMetadataByDataOffset(brickOffset);
      logger.warn("found an unit, offset {}, metadataOffset {}", brickOffset, metadataOffset);
      SegId segId = new SegId(archive.getArchiveMetadata().getArchiveId(), (int) brickOffset);
      BrickMetadata newBrickMetadata = new BrickMetadata(segId, brickOffset, metadataOffset,
          BrickStatus.free, archive.getStorage(), ArchiveOptions.PAGE_NUMBER_PER_SEGMENT,
          ArchiveOptions.PAGE_NUMBER_PER_SEGMENT);
      BrickSpaceManager brickSpaceManager = new BrickSpaceManagerImpl(newBrickMetadata);
      newBrickMetadata.setBrickSpaceManager(brickSpaceManager);
      try {
        newBrickMetadata.clear(true);

        newBrickMetadata.setIsPreAllocated(true);
        persistBrickMetadata(newBrickMetadata, completionHandler);
        return;
      } catch (Exception e) {
        logger.error("catch an exception when allocate an free page", e);
      }
    } catch (NotEnoughSpaceException e) {
      logger.warn("cannot find free space for a new archive unit, storage {}, bricks {}",
          this.archive.getArchiveMetadata(), this.mapOffsetToBrick, e);
    }

    throw new NotEnoughSpaceException(
        "can not get an page from archive: " + archive.getArchiveMetadata());
  }

  private boolean unitCanBeReused(AbstractSegmentUnitMetadata unitMetadata) {
    return unitMetadata.isUnitReusable();
  }

  private boolean brickCanBeReused(BrickMetadata brickMetadata, boolean needCheckPreAllocated) {
    if (brickMetadata.isUnitReusable() && !brickMetadata.getMarkedBrickCannotAllocatePages()) {
      if (needCheckPreAllocated && brickMetadata.isPreAllocated()) {
        return false;
      }
      return true;
    }

    return false;
  }

  private void recordUnitMetadataAllocated(AbstractSegmentUnitMetadata unitMetadata) {
    int unitIndex = (int) (
        (unitMetadata.getLogicalDataOffset() - archive.getSegmentUnitLogicalDataStartPosition())
            / ArchiveOptions.SEGMENT_PHYSICAL_SIZE);

    Validate.isTrue(unitIndex >= 0 && unitIndex < ArchiveOptions.MAX_FLEXIBLE_COUNT);
    unitMetadataAllocateBitSet.set(unitIndex);

    mapOffsetToArchiveUnit.put(unitMetadata.getLogicalDataOffset(), unitMetadata);
  }

  private void recordBrickMetadataAllocated(BrickMetadata brickMetadata) {
    int brickIndex = (int) (
        (brickMetadata.getDataOffset() - archive.getSegmentUnitDataStartPosition())
            / ArchiveOptions.SEGMENT_PHYSICAL_SIZE);

    Validate
        .isTrue(brickIndex >= 0 && brickIndex < archive.getArchiveMetadata().getMaxSegUnitCount());
    brickMetadataAllocateBitSet.set(brickIndex);

    mapOffsetToBrick.put(brickMetadata.getDataOffset(), brickMetadata);
  }

  private void preProcessCanReusableMetadata() {
    mapOffsetToArchiveUnit.values().stream().forEach(segmentUnitMetadata -> {
      logger.info("begin process reusable metadata {}", segmentUnitMetadata);
      if (unitCanBeReused(segmentUnitMetadata)) {
        if (!canReusedUnitMetadata.contains(segmentUnitMetadata)) {
          canReusedUnitMetadata.add(segmentUnitMetadata);
          logger.warn("reusable metadata {}", segmentUnitMetadata);
        }
      } else {
        logger.info("can not reusable metadata {}", segmentUnitMetadata);
      }
    });

    mapOffsetToBrick.values().stream().forEach(brickMetadata -> {
      if (brickCanBeReused(brickMetadata, true)) {
        canReusedBrickMetadata.add(brickMetadata);
      }
    });
  }
}
