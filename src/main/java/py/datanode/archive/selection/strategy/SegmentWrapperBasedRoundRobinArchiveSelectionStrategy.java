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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import py.archive.segment.SegId;
import py.archive.segment.SegmentUnitType;
import py.datanode.archive.RawArchive;
import py.datanode.exception.InsufficientFreeSpaceException;
import py.datanode.segment.SegmentUnit;
import py.datanode.segment.SegmentUnitManager;
import py.instance.InstanceId;
import py.membership.SegmentMembership;
import py.volume.VolumeId;

public class SegmentWrapperBasedRoundRobinArchiveSelectionStrategy extends
    RoundRobinArchiveSelectionStrategy {
  private final SegmentUnitManager segmentUnitManager;
  private final HashSet<VolumeId> needToRebuildCounter = new HashSet<>();

  public SegmentWrapperBasedRoundRobinArchiveSelectionStrategy(
      SegmentUnitManager segmentUnitManager, InstanceId myself) {
    super(myself);
    this.segmentUnitManager = segmentUnitManager;
  }

  @Override
  public synchronized RawArchive selectArchive(Collection<RawArchive> archives, long storagePoolId,
      VolumeId volumeId,
      int segmentIndex, int segmentWrapSize, SegmentUnitType segmentUnitType,
      SegmentMembership membership) throws InsufficientFreeSpaceException {
    Set<RawArchive> allocatedArchivesInTheCurrentWrap = new HashSet<>();
    int wrapperIndex = segmentIndex / segmentWrapSize;
    for (int i = 0; i < segmentWrapSize; i++) {
      SegId segId = new SegId(volumeId, wrapperIndex * segmentWrapSize + i);
      SegmentUnit segmentUnit = segmentUnitManager.get(segId);
      if (segmentUnit != null && !segmentUnit.isArbiter()) {
        allocatedArchivesInTheCurrentWrap.add(segmentUnit.getArchive());
      }
    }

    VolumeArchiveLayout layout = mapVolumeIdToLayout
        .computeIfAbsent(volumeId, k -> new VolumeArchiveLayout(k, myself));
    if (needToRebuildCounter.remove(volumeId)) {
      layout.clear();
      segmentUnitManager
          .get(segmentUnit -> volumeId.equals(segmentUnit.getSegId().getVolumeId()) && !segmentUnit
              .getSegmentUnitMetadata().getStatus().isFinalStatus()).forEach(segmentUnit -> layout
          .increment(archives, segmentUnit.getSegmentUnitMetadata().getMembership(),
              segmentUnit.getArchive()));
    }

    Iterator<RawArchive> archiveIterator = layout.iterator(archives, membership);

    RawArchive archiveCandidate = null;
    while (archiveIterator.hasNext()) {
      RawArchive archive = archiveIterator.next();
      if (archive != null && archive.getStoragePoolId() != null
          && archive.getStoragePoolId() == storagePoolId
          && archive.acceptNewSegmentUnitCreation(segmentUnitType)) {
        if (archiveCandidate == null) {
          archiveCandidate = archive;
        }
        if (!allocatedArchivesInTheCurrentWrap.contains(archive)) {
          layout.increment(archives, membership, archive);
          return archive;
        }
      }
    }
    if (archiveCandidate == null) {
      throw new InsufficientFreeSpaceException("no available archives could be selected");
    } else {
      layout.increment(archives, membership, archiveCandidate);
      return archiveCandidate;
    }
  }

  @Override
  public synchronized RawArchive selectArchive(Collection<RawArchive> archives, long storagePoolId)
      throws InsufficientFreeSpaceException {
    throw new UnsupportedOperationException();
  }

  @Override
  public synchronized void removeSegmentUnit(SegmentUnit segmentUnit, RawArchive archive) {
    needToRebuildCounter.add(segmentUnit.getSegId().getVolumeId());
  }
}
