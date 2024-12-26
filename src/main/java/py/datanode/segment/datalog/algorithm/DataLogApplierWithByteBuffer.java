

package py.datanode.segment.datalog.algorithm;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

public class DataLogApplierWithByteBuffer extends DataLogsApplier {
  private ByteBuffer destination;
  private int initPosition;

  public DataLogApplierWithByteBuffer(long destinationPos, int destinationLength, int pageSize) {
    super(destinationPos, destinationLength, pageSize);
  }

  @Override
  protected void applyLogData(DataLog log, int offsetInDestination, int offsetInLog,
      int length) {
    destination.position(initPosition + offsetInDestination);
    log.getData(destination, offsetInLog, length);
  }

  @Override
  protected CompletableFuture<Void> loadPageData(boolean wholePageCovered) {
    return CompletableFuture.completedFuture(null);
  }

  public void setDestination(ByteBuffer destination) {
    if (this.destination != null) {
      throw new IllegalArgumentException("destination already set");
    }
    this.destination = destination;
    this.initPosition = destination.position();
  }
}
