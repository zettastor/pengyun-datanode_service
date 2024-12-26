

package py.datanode.segment.datalog;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import py.archive.segment.SegId;
import py.common.struct.Pair;

public abstract class MutationLogEntryReader implements Closeable {
  public abstract  void open(InputStream inputStream) throws IOException;

  public abstract  Pair<SegId, MutationLogEntry> readLogAndSegment() throws IOException;

  public abstract  MutationLogEntry read() throws IOException;

  protected abstract MutationLogEntrySerializationCompactFormat.FieldType[] getFormat();

  public abstract DataInputStream getInputStream();
}
