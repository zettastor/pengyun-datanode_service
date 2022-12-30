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

package py.datanode.segment.membership.statemachine;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.segment.SegId;
import py.archive.segment.SegmentUnitMetadata;

public class GlobalDeletingSegmentUnitCollection {
  private static final Logger logger = LoggerFactory
      .getLogger(GlobalDeletingSegmentUnitCollection.class);
  private static final Map<SegId, DeletingSegmentUnitCollection> 
      segIdDeletingSegmentUnitCollectionMap = new ConcurrentHashMap<>();

  public GlobalDeletingSegmentUnitCollection() {
  }

  public static void newDeletingSegmentUnit(SegmentUnitMetadata segmentUnitMetadata) {
    segIdDeletingSegmentUnitCollectionMap.put(
        segmentUnitMetadata.getSegId(),
        new DeletingSegmentUnitCollection(DeletingSegmentUnitStatus.Begin, segmentUnitMetadata)
    );
  }

  public static DeletingSegmentUnitCollection getDeletingSegmentUnitCollection(SegId segId) {
    return segIdDeletingSegmentUnitCollectionMap.get(segId);
  }

  public static DeletingSegmentUnitCollection getAndRemoveDeletingSegmentUnitCollection(
      SegId segId) {
    return segIdDeletingSegmentUnitCollectionMap.remove(segId);
  }

  public static Map<SegId, DeletingSegmentUnitCollection> buildCurrentHashMap() {
    Map<SegId, DeletingSegmentUnitCollection> hashMap = new HashMap<>(
        segIdDeletingSegmentUnitCollectionMap);

    return hashMap;
  }

  public static void updateFailedStatus(
      Map<SegId, DeletingSegmentUnitCollection> segmentUnitCollectionMap) {
    logger.warn("update deleting segment units to failed {}", segmentUnitCollectionMap);
    for (Map.Entry entry : segmentUnitCollectionMap.entrySet()) {
      SegId segId = (SegId) entry.getKey();
      DeletingSegmentUnitCollection deletingSegmentUnitCollection =
          (DeletingSegmentUnitCollection) entry
          .getValue();
      deletingSegmentUnitCollection.status = DeletingSegmentUnitStatus.Failed;

      if (null == segIdDeletingSegmentUnitCollectionMap
          .replace(segId, deletingSegmentUnitCollection)) {
        logger.error(
            "update deleting segment unit status failed, can not found any old data in static map");
      }
    }
  }

  public static void updateSuccessStatus(
      Map<SegId, DeletingSegmentUnitCollection> segmentUnitCollectionMap) {
    logger.debug("update deleting segment units to success {}", segmentUnitCollectionMap);
    for (Map.Entry entry : segmentUnitCollectionMap.entrySet()) {
      SegId segId = (SegId) entry.getKey();
      DeletingSegmentUnitCollection deletingSegmentUnitCollection = 
          (DeletingSegmentUnitCollection) entry
          .getValue();
      deletingSegmentUnitCollection.status = DeletingSegmentUnitStatus.Success;

      if (null == segIdDeletingSegmentUnitCollectionMap
          .replace(segId, deletingSegmentUnitCollection)) {
        logger.error(
            "update deleting segment unit status success, " 
                + "can not found any old data in static map");
      }
    }
  }

  public enum DeletingSegmentUnitStatus {
    Begin,
    Failed,
    Success
  }

  public static class DeletingSegmentUnitCollection {
    public DeletingSegmentUnitStatus status = DeletingSegmentUnitStatus.Begin;
    public SegmentUnitMetadata segmentUnitMetadata;

    public DeletingSegmentUnitCollection(DeletingSegmentUnitStatus status,
        SegmentUnitMetadata segmentUnitMetadata) {
      this.status = status;
      this.segmentUnitMetadata = segmentUnitMetadata;
    }

    public DeletingSegmentUnitStatus getStatus() {
      return status;
    }

    public void setStatus(DeletingSegmentUnitStatus status) {
      this.status = status;
    }

    public SegmentUnitMetadata getSegmentUnitMetadata() {
      return segmentUnitMetadata;
    }

    public void setSegmentUnitMetadata(SegmentUnitMetadata segmentUnitMetadata) {
      this.segmentUnitMetadata = segmentUnitMetadata;
    }
  }
}
