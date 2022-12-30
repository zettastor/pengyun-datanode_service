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

import py.archive.AbstractSegmentUnitMetadata;
import py.datanode.archive.RawArchive;

public class PersistDataContextFactory {
  public static PersistDataContext generatePersistDataContext(
      AbstractSegmentUnitMetadata abstractSegmentUnitMetadata,
      PersistDataType workType) {
    PersistDataContext context = new PersistDataContext(abstractSegmentUnitMetadata, workType,
        false);
    return context;
  }

  public static PersistDataContext generatePersistDataContext(
      AbstractSegmentUnitMetadata abstractSegmentUnitMetadata,
      PersistDataType workType, RawArchive rawArchive) {
    PersistDataContext context = new PersistDataContext(abstractSegmentUnitMetadata, workType,
        false, rawArchive);
    return context;
  }

}
