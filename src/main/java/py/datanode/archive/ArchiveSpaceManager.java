/**
* Copyright (C) 2013-2024 Nanjing Pengyun Network Technology Co., Ltd.
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
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