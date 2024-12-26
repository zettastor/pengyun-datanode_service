
package py.datanode.service.io;

import py.datanode.segment.SegmentUnit;

public abstract class IoTask implements Runnable {
  protected final SegmentUnit segmentUnit;

  protected IoTask(SegmentUnit segmentUnit) {
    this.segmentUnit = segmentUnit;
  }

  public SegmentUnit getSegmentUnit() {
    return segmentUnit;
  }

}
