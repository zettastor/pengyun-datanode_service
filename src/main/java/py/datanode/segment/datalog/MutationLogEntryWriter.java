
package py.datanode.segment.datalog;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import py.archive.segment.SegId;

/**
 * A writer to write a mutation log entry to an output stream.
 *
 */
public abstract class MutationLogEntryWriter implements Closeable {
  /**
   * Open the writer.
   *
   */
  public abstract void open(OutputStream outputStream) throws IOException;

  public abstract int write(MutationLogEntry log) throws IOException;

  /**
   * write a log.
   *
   */
  public abstract int write(SegId segId, MutationLogEntry log) throws IOException;

  protected abstract ByteBuffer putLogHeaders(SegId segId, MutationLogEntry log);

  public abstract OutputStream getOutputStream();
}
