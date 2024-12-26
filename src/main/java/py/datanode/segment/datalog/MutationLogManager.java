

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
