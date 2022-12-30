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
