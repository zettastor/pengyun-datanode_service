
package py.datanode.service.io.throttle.strategy;

import py.datanode.configuration.DataNodeConfiguration;

public class StrictIoThrottleStrategy extends IoThrottleStrategy {
  public StrictIoThrottleStrategy(DataNodeConfiguration cfg) {
    super(cfg);
  }

}
