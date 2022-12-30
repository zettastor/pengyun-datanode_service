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

package py.datanode.segment.datalog;

import java.util.List;
import py.archive.segment.SegId;
import py.datanode.exception.SegmentLogMetadataNotExistException;
import py.datanode.segment.datalogbak.catchup.SegmentLogQualifier;

/**
 * All operations to logs for a segment should be synchronized.
 *
 */
public interface MutationLogManager {
  public void setLogIdGenerator(SegId segId, LogIdGenerator generator)
      throws SegmentLogMetadataNotExistException;

  public List<SegmentLogMetadata> getSegments(SegmentLogQualifier qualifier);

  public SegmentLogMetadata getSegment(SegId segId);

  public void removeSegmentLogMetadata(SegId segId);

  public SegmentLogMetadata createOrGetSegmentLogMetadata(SegId segId, int quorumSize);

  // for testing purpose
  public void setSegmentLogMetadata(SegmentLogMetadata logMetadata);
}
