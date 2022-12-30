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
import py.archive.segment.SegmentUnitType;
import py.datanode.exception.InsufficientFreeSpaceException;
import py.datanode.segment.SegmentUnit;
import py.membership.SegmentMembership;
import py.volume.VolumeId;

public interface ArchiveSelectionStrategy<A> {
  A selectArchive(Collection<A> archives, long storagePoolId, VolumeId volumeId,
      SegmentUnitType segmentUnitType, SegmentMembership membership)
      throws InsufficientFreeSpaceException;

  A selectArchive(Collection<A> archives, long storagePoolId, VolumeId volumeId, int segIndex,
      int segmentWrapSize,
      SegmentUnitType segmentUnitType, SegmentMembership membership)
      throws InsufficientFreeSpaceException;

  /**
   * Select an archive from the given archive list.
   */
  A selectArchive(Collection<A> archives, long storagePoolId) throws InsufficientFreeSpaceException;

  /**
   * notify that a segment unit has been or is being removed.
   */
  void removeSegmentUnit(SegmentUnit segmentUnit, A a);
}
