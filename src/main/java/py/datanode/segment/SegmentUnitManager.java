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

import java.util.Collection;
import java.util.function.Predicate;
import py.archive.segment.SegId;
import py.datanode.archive.RawArchive;

public interface SegmentUnitManager {
  /**
   * retrieve all segment units.
   *
   */
  Collection<SegmentUnit> get();

  SegmentUnit get(SegId segId);

  Collection<SegmentUnit> get(Predicate<SegmentUnit> condition);
  
  RawArchive getArchive(SegId segId) throws IllegalArgumentException;

  void put(SegmentUnit unit);

  /**
   * Add all segment units in the collection.
   */
  void putAll(Collection<SegmentUnit> unit);

  /**
   * Remove a segment unit.
   */
  SegmentUnit remove(SegId seg);

  /**
   * Remove all segment units specified in the collection.
   */
  void removeAll(Collection<SegId> segIds);

  void addListener(AddOrRemoveSegmentUnitListener listener);
}
