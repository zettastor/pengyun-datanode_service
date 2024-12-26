

package py.datanode.service.io;

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import py.datanode.segment.SegmentUnit;

public abstract class DelayedIoTask extends IoTask implements Delayed {
  private long deadline;

  public DelayedIoTask(int delayMs, SegmentUnit segmentUnit) {
    super(segmentUnit);
    this.deadline = System.currentTimeMillis() + delayMs;
  }

  @Override
  public long getDelay(TimeUnit unit) {
    return unit.convert(deadline - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
  }

  @Override
  public int compareTo(Delayed o) {
    return (int) (getDelay(TimeUnit.MILLISECONDS) - o.getDelay(TimeUnit.MILLISECONDS));
  }

  public void updateDelay(int delayMs) {
    deadline = System.currentTimeMillis() + delayMs;
  }
}
