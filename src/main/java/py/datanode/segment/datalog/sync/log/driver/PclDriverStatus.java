
package py.datanode.segment.datalog.sync.log.driver;

public enum PclDriverStatus {
  Starting,
  Free {
    @Override
    public boolean isFreeStatus() {
      return true;
    }
  },
  Processing,
  Waiting,
  Expired {
    @Override
    public boolean isFreeStatus() {
      return true;
    }
  };

  public boolean isFreeStatus() {
    return false;
  }
}
