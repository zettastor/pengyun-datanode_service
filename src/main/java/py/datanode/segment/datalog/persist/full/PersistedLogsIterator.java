
package py.datanode.segment.datalog.persist.full;

import py.datanode.segment.datalog.MutationLogEntry;

public interface PersistedLogsIterator {
  public MutationLogEntry next();

  public boolean hasNext();
}
