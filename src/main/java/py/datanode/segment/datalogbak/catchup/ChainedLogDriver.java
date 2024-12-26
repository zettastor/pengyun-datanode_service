

package py.datanode.segment.datalogbak.catchup;

public interface ChainedLogDriver extends LogDriver {
  public ChainedLogDriver getNextDriver();

  public void setNextDriver(ChainedLogDriver driver);
}
