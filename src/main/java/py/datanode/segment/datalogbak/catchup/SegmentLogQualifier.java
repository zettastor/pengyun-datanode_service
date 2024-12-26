

package py.datanode.segment.datalogbak.catchup;

import py.datanode.segment.datalog.SegmentLogMetadata;

public interface SegmentLogQualifier {
  public boolean isQaulified(SegmentLogMetadata segmentLogMetadata);
}
