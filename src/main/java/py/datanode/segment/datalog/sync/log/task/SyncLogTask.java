
package py.datanode.segment.datalog.sync.log.task;

import java.util.concurrent.atomic.AtomicInteger;
import py.archive.segment.SegId;
import py.instance.InstanceId;

public interface SyncLogTask {
  AtomicInteger syncLogTaskIdGenerator = new AtomicInteger(0);
  int syncLogTaskId = syncLogTaskIdGenerator.getAndIncrement();

  /**
   * process sync log task unit.
   *
   * @return true if success
   */
  public boolean process();

  /**
   * after process sync log task unit, using this function to collect all result info.
   *
   * @return true if success
   */
  public boolean reduce();

  public InstanceId homeInstanceId();

  public SyncLogTaskType type();

  public SegId getSegId();
}
