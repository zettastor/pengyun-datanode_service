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

package py.datanode.segment;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.segment.SegId;

public class PersistDataContextWithPriority {
  private static final Logger logger = LoggerFactory
      .getLogger(PersistDataContextWithPriority.class);
  PersistDataContext persistDataContext;
  PersistDataContext persistShadowContext;
  AtomicInteger priority;
  SegId segId;

  public PersistDataContextWithPriority() {
    this.priority = new AtomicInteger(0);
  }

  public PersistDataContext getPersistDataContext() {
    return persistDataContext;
  }

  public AtomicInteger getPriority() {
    return priority;
  }

  public void setPriority(AtomicInteger priority) {
    this.priority = priority;
  }

  public void updataPersistContext(PersistDataContext newContext) {
    if (segId == null) {
      segId = newContext.getAbstractSegmentUnitMetadata().getSegId();
    }
    switch (newContext.getPersistWorkType()) {
      case ShadowUnitMetadata:
        updataShadowContext(newContext);
        break;
      case Bitmap:
      case SegmentUnitMetadata:
      case SegmentUnitMetadataAndBitMap:
        updataPersistBitMapAndSegmentUnitMetadataContext(newContext);
        break;
      default:
        throw new NotImplementedException("PersistWorkType not implemented");
    }

  }

  private void updataPersistBitMapAndSegmentUnitMetadataContext(PersistDataContext newContext) {
    if (newContext.getPersistWorkType() == PersistDataType.ShadowUnitMetadata) {
      logger.error("updataPersistBitMapAndSegmentUnitMetadataContext {} error ", newContext);
      Validate.isTrue(false);
    }
    if (!newContext.getAbstractSegmentUnitMetadata().getSegId().equals(segId)) {
      logger.error("the new Context {} is not equal with segID {}",
          newContext.getAbstractSegmentUnitMetadata().getSegId(), segId);
      Validate.isTrue(false);
    }
    if (persistDataContext == null) {
      persistDataContext = newContext;
    } else {
      if (persistDataContext.getPersistWorkType() != newContext.getPersistWorkType()) {
        newContext.setPersistWorkType(PersistDataType.SegmentUnitMetadataAndBitMap);
        persistDataContext = newContext;
      } else {
        persistDataContext = newContext;
      }
    }
    priority.getAndIncrement();
  }

  private void updataShadowContext(PersistDataContext newContext) {
    Validate.isTrue(newContext.getPersistWorkType() == PersistDataType.ShadowUnitMetadata);
    if (!newContext.getAbstractSegmentUnitMetadata().getSegId().equals(segId)) {
      logger.error("the new Context {} is not equal with segID {}",
          newContext.getAbstractSegmentUnitMetadata().getSegId(), segId);
      Validate.isTrue(false);
    }
    persistShadowContext = newContext;
    priority.getAndIncrement();
  }

  public List<PersistDataContext> getAllContext() {
    List<PersistDataContext> allContext = new ArrayList<>();
    if (persistDataContext != null) {
      allContext.add(persistDataContext);
    }
    if (persistShadowContext != null) {
      allContext.add(persistShadowContext);
    }

    return allContext;
  }

  public SegId getSegId() {
    return segId;
  }

}
