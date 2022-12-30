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

import java.util.Collection;
import java.util.List;
import py.archive.AbstractSegmentUnitMetadata;
import py.archive.brick.BrickMetadata;
import py.archive.page.MultiPageAddress;
import py.archive.page.PageAddress;
import py.archive.segment.CloneType;
import py.archive.segment.SegId;
import py.archive.segment.SegmentUnitMetadata;
import py.archive.segment.SegmentUnitType;
import py.archive.segment.recurring.MigratingSegmentUnitMetadata;
import py.datanode.exception.CannotAllocMoreArbiterException;
import py.datanode.exception.CannotAllocMoreFlexibleException;
import py.exception.NotEnoughSpaceException;
import py.exception.StorageException;
import py.instance.InstanceId;
import py.membership.SegmentMembership;
import py.volume.CacheType;
import py.volume.VolumeType;

interface ArchiveSpaceManager {
  SegmentUnitMetadata allocateSegmentUnit(SegId segId, SegmentMembership membership,
      VolumeType volumeType,
      String volumeMetadataJson, String accountMetadataJson,
      SegmentUnitType segmentUnitType,
      long srcVolumeId,  SegmentMembership srcMembership,
      boolean isSecondaryCandidate, InstanceId replacee)
      throws NotEnoughSpaceException, CannotAllocMoreArbiterException, 
      CannotAllocMoreFlexibleException;

  void replaceAbstractSegmentUnitMetadata(AbstractSegmentUnitMetadata from,
      AbstractSegmentUnitMetadata to);

  MigratingSegmentUnitMetadata allocateMigratingSegmentUnit(SegId sourceSegId)
      throws StorageException, NotEnoughSpaceException;

  boolean freeAbstractSegmentUnitMetadata(AbstractSegmentUnitMetadata abstractSegmentUnitMetadata);

  long getUsedSpace();

  long getFreeBrickSpace();

  long getTotalSpace();

  boolean acceptNewSegmentUnitCreation();

  long getLargestContiguousFreeSpaceLogically();

  void persistBitMap();

  int getArbiterInUsedCount();

  int getFlexibleInUsedCount();

  int getNormalInUsedCount();

  int getSegmentUnitInUsedCount();

  boolean isFlexibleLogicalAddress(long dataOffsetInArchive);

  Collection<BrickMetadata> getAllBrickMetadata();

  AbstractSegmentUnitMetadata getArchiveUnitByOffset(long offset);

  BrickMetadata getArchiveBrickByOffset(long offset);

  List<BrickMetadata> getShadowUnitList();

  boolean markBrickInGarbageCollection(BrickMetadata brickMetadata, boolean marked);

  void persistCleanDirtyFlag();

  void preAllocateBrick(int lowerThreshold, int upperThreshold, boolean isArbitPreAllocator);

  long getBrickStartOffset(long physicalOffsetOnArchive);
}