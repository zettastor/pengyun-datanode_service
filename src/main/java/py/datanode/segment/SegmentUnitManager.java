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
