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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.segment.SegId;
import py.archive.segment.SegmentUnitType;
import py.datanode.archive.RawArchive;
import py.datanode.exception.InsufficientFreeSpaceException;
import py.datanode.segment.SegmentUnit;
import py.instance.InstanceId;
import py.membership.SegmentMembership;
import py.volume.VolumeId;

public class RoundRobinArchiveSelectionStrategy implements ArchiveSelectionStrategy<RawArchive> {
  @SuppressWarnings("unused")
  private static final Logger logger = LoggerFactory
      .getLogger(RoundRobinArchiveSelectionStrategy.class);

  protected final InstanceId myself;

  protected final Map<VolumeId, VolumeArchiveLayout> mapVolumeIdToLayout = new HashMap<>();

  public RoundRobinArchiveSelectionStrategy(InstanceId myself) {
    this.myself = myself;
  }

  @Override
  public synchronized RawArchive selectArchive(Collection<RawArchive> archives, long storagePoolId,
      VolumeId volumeId,
      SegmentUnitType segmentUnitType, SegmentMembership membership)
      throws InsufficientFreeSpaceException {
    VolumeArchiveLayout layout = mapVolumeIdToLayout
        .computeIfAbsent(volumeId, k -> new VolumeArchiveLayout(k, myself));
    Iterator<RawArchive> archiveIterator = layout.iterator(archives, membership);

    while (archiveIterator.hasNext()) {
      RawArchive archive = archiveIterator.next();
      if (archive != null && archive.getStoragePoolId() != null
          && archive.getStoragePoolId() == storagePoolId
          && archive.acceptNewSegmentUnitCreation(segmentUnitType)) {
        layout.increment(archives, membership, archive);
        return archive;
      }
    }
    throw new InsufficientFreeSpaceException("no available archives could be selected");
  }

  @Override
  public synchronized RawArchive selectArchive(Collection<RawArchive> archives, long storagePoolId,
      VolumeId volumeId,
      int segIndex, int segmentWrapSize, SegmentUnitType segmentUnitType,
      SegmentMembership membership) throws InsufficientFreeSpaceException {
    return selectArchive(archives, storagePoolId, volumeId, segmentUnitType, membership);
  }

  @Override
  public synchronized RawArchive selectArchive(Collection<RawArchive> archives, long storagePoolId)
      throws InsufficientFreeSpaceException {
    throw new UnsupportedOperationException();
  }

  @Override
  public synchronized void removeSegmentUnit(SegmentUnit segmentUnit, RawArchive archive) {
    SegId segId = segmentUnit.getSegId();
    VolumeArchiveLayout layout = mapVolumeIdToLayout.get(segId.getVolumeId());
    if (layout != null) {
      layout.decrement(segmentUnit.getSegmentUnitMetadata().getMembership(), archive);
    }
  }

}
