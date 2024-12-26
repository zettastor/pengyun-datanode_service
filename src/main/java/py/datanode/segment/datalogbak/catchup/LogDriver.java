
package py.datanode.segment.datalogbak.catchup;

public interface LogDriver {
  ExecuteLevel drive() throws Exception;

  enum ExecuteLevel {
    IMMEDIATELY(0),
    QUICKLY(10),
    SLOWLY(2000);

    final int delay;

    ExecuteLevel(int delay) {
      this.delay = delay;
    }

    public int getDelay() {
      return delay;
    }
  }
}
