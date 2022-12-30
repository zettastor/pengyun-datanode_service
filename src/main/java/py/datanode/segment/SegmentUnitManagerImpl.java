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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.segment.SegId;
import py.archive.segment.recurring.SegmentUnitTaskCallback;
import py.archive.segment.recurring.SegmentUnitTaskContext;
import py.datanode.archive.RawArchive;
import py.datanode.segment.datalogbak.catchup.CatchupLogContext;
import py.datanode.segment.membership.statemachine.processors.StateProcessingContext;

public class SegmentUnitManagerImpl implements SegmentUnitManager, SegmentUnitTaskCallback,
    ArbiterManager {
  private static final Logger logger = LoggerFactory.getLogger(SegmentUnitManagerImpl.class);
  private final List<AddOrRemoveSegmentUnitListener> listeners;
  private Map<SegId, SegmentUnit> segmentUnits;
  private AtomicInteger countOfArbiters;

  public SegmentUnitManagerImpl() {
   
   
   
    segmentUnits = new ConcurrentHashMap<>(256 * 30);
    countOfArbiters = new AtomicInteger(0);
    listeners = new ArrayList<>();
  }

  @Override
  public Collection<SegmentUnit> get() {
    return get(segmentUnit -> true);
  }

  @Override
  public Collection<SegmentUnit> get(Predicate<SegmentUnit> condition) {
    List<SegmentUnit> units = new ArrayList<>();
    for (Entry<SegId, SegmentUnit> entry : segmentUnits.entrySet()) {
      if (condition.test(entry.getValue())) {
        units.add(entry.getValue());
      }
    }
    return units;
  }
  
  @Override
  public SegmentUnit get(SegId segId) {
    return segmentUnits.get(segId);
  }

  @Override
  public void put(SegmentUnit newUnit) {
    logger.info("put the new unit {}  metadata:{} archive:{} to the map", newUnit.getSegId(),
        newUnit.getSegmentUnitMetadata(), newUnit.getArchive());
    if (newUnit.isArbiter()) {
      countOfArbiters.incrementAndGet();
    }
    segmentUnits.put(newUnit.getSegId(), newUnit);
    listeners.forEach(listener -> listener.segmentUnitAdded(newUnit));
  }

  @Override
  public RawArchive getArchive(SegId segId) throws IllegalArgumentException {
    SegmentUnit segmentUnit = segmentUnits.get(segId);
    if (segmentUnit != null) {
      return segmentUnit.getArchive();
    } else {
      return null;
    }
  }

  @Override
  public void putAll(Collection<SegmentUnit> units) {
    if (units != null) {
      for (SegmentUnit unit : units) {
        put(unit);
      }
    }
  }

  @Override
  public SegmentUnit remove(SegId segId) {
    SegmentUnit removeUnit = segmentUnits.remove(segId);
    if (removeUnit != null) {
      listeners.forEach(listener -> listener.segmentUnitRemoved(removeUnit));
      if (removeUnit.isArbiter()) {
        countOfArbiters.decrementAndGet();
      }
    }
    return removeUnit;
  }

  @Override
  public void removeAll(Collection<SegId> segIds) {
    if (segIds != null) {
      for (SegId segId : segIds) {
        remove(segId);
      }
    }
  }

  @Override
  public void addListener(AddOrRemoveSegmentUnitListener listener) {
    listeners.add(listener);
  }

  @Override
  public void removing(SegmentUnitTaskContext targetContext) {
    SegmentUnit unit = null;
    SegId segId = targetContext.getSegId();
    logger.warn("removing a context {} that might be related to a deleted segment unit {}",
        targetContext, segId);
    if (targetContext instanceof StateProcessingContext) {
      unit = ((StateProcessingContext) targetContext).getSegmentUnit();
      unit.setStateContextsHaveBeenRemoved();
    } else if (targetContext instanceof CatchupLogContext) {
      unit = ((CatchupLogContext) targetContext).getSegmentUnit();
      unit.setLogContextsHaveBeenRemoved();
    } else {
      logger.error("the context {} is not the type that we know", targetContext);
      return;
    }

    if (unit.isLogContextsHaveBeenRemoved() && unit.isStateContextsHaveBeenRemoved()) {
      logger.warn(
          "all contexts related to have been removed. " 
              + "Remove it from SegmentUnitManager for segId={}",
          segId);
      remove(segId);
    } else {
      logger.debug("There are still some contexts related to the context. Not remove");
    }

    targetContext.setAbandonedTask(true);
  }

  @Override
  public int getCountOfArbiters() {
    return countOfArbiters.get();
  }

  public void setCountOfArbiters(int countOfArbiters) {
    this.countOfArbiters.set(countOfArbiters);
  }
}
