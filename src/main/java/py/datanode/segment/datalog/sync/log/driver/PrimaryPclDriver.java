

package py.datanode.segment.datalog.sync.log.driver;

import py.datanode.segment.datalogbak.catchup.LogDriver;

public class PrimaryPclDriver extends PclDriver implements LogDriver {
  public PrimaryPclDriver(int timeout) {
    super(timeout);
  }

  @Override
  public Boolean call() throws Exception {
    return drive() == ExecuteLevel.SLOWLY;
  }

  @Override
  public ExecuteLevel drive() throws Exception {
    return ExecuteLevel.SLOWLY;
  }

}
