

package py.datanode.segment.heartbeat;

import py.datanode.segment.SegmentUnit;

public interface HostHeartbeat {
  public void addRelationBetweenPrimaryAndAllAliveSecondaries(SegmentUnit segUnit);

  public void removeRelationBetweenPrimaryAndAllAliveSecondaries(SegmentUnit segUnit);

  public void addRelationBetweenPrimaryAndAliveSecondary(SegmentUnit segunit,
      long aliveSecondaryInstanceId);

  public void removeRelationBetweenPrimaryAndAliveSecondary(SegmentUnit segunit,
      long aliveSecondaryInstanceId);

  public void start();

  public void stop();

  public void pause();

  public void restart();
}
