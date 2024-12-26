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

package py.datanode.archive.selection.strategy;

import java.util.Collection;
import java.util.Iterator;
import py.archive.ArchiveStatus;
import py.archive.segment.SegmentUnitType;
import py.datanode.archive.RawArchive;
import py.datanode.exception.InsufficientFreeSpaceException;
import py.datanode.segment.SegmentUnit;
import py.membership.SegmentMembership;
import py.volume.VolumeId;

public class FreeSpaceBasedArchiveSelectionStrategy implements
    ArchiveSelectionStrategy<RawArchive> {
  @Override
  public RawArchive selectArchive(Collection<RawArchive> archives, long storagePoolId,
      VolumeId volumeId,
      SegmentUnitType segmentUnitType, SegmentMembership membership)
      throws InsufficientFreeSpaceException {
    throw new UnsupportedOperationException();
  }

  @Override
  public RawArchive selectArchive(Collection<RawArchive> archives, long storagePoolId,
      VolumeId volumeId,
      int segIndex, int segmentWrapSize, SegmentUnitType segmentUnitType,
      SegmentMembership membership) throws InsufficientFreeSpaceException {
    throw new UnsupportedOperationException();
  }

  @Override
  public RawArchive selectArchive(Collection<RawArchive> archives, long storagePoolId)
      throws InsufficientFreeSpaceException {
    Iterator<RawArchive> iterator = archives.iterator();
    RawArchive archiveWithLargestFreeSpace = null;
    while (iterator.hasNext()) {
      RawArchive archive = iterator.next();
      if (archiveWithLargestFreeSpace == null) {
        archiveWithLargestFreeSpace = archive;
        continue;
      }

      if (archive.getStoragePoolId() == null || archive.getStoragePoolId() != storagePoolId) {
        continue;
      }

      if (archive.getArchiveStatus() == ArchiveStatus.DEGRADED) {
        continue;
      }

      if (archiveWithLargestFreeSpace.getLogicalFreeSpace() < archive.getLogicalFreeSpace()) {
        archiveWithLargestFreeSpace = archive;
      }
    }
    if (archiveWithLargestFreeSpace == null || !archiveWithLargestFreeSpace
        .acceptNewSegmentUnitCreation(SegmentUnitType.Normal)) {
      throw new InsufficientFreeSpaceException("no available archives coule be selected");
    } else {
      return archiveWithLargestFreeSpace;
    }
  }

  @Override
  public void removeSegmentUnit(SegmentUnit segmentUnit, RawArchive rawArchive) {
   
  }

}
