
package py.datanode.segment.membership;

public class NewPrimaryProposal {
  private final long primaryId;
  private final int lease;

  public NewPrimaryProposal(long primaryId, int lease) {
    super();
    this.primaryId = primaryId;
    this.lease = lease;
  }

  public long getPrimaryId() {
    return primaryId;
  }

  public int getLease() {
    return lease;
  }
}
