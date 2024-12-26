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
