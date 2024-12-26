

package py.datanode.segment;

public interface AddOrRemoveSegmentUnitListener {
  void segmentUnitAdded(SegmentUnit segmentUnit);

  void segmentUnitRemoved(SegmentUnit segmentUnit);

}
