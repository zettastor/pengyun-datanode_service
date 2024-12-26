
package py.datanode.segment.datalog.algorithm;

import io.netty.buffer.ByteBuf;
import java.nio.ByteBuffer;
import py.datanode.segment.datalog.MutationLogEntry.LogStatus;

public class AppliedLog extends ImmutableStatusDataLog {
  public AppliedLog(long logId, long offset, int length, long logUuid) {
    super(logId, offset, length, logUuid,  LogStatus.Committed, true, false);
  }

  @Override
  public void getData(ByteBuffer destination, int offset, int length) {
    throw new UnsupportedOperationException("you can't get data from an applied log");
  }

  @Override
  public void getData(ByteBuf destination, int offset, int length) {
    throw new UnsupportedOperationException("you can't get data from an applied log");
  }

}
