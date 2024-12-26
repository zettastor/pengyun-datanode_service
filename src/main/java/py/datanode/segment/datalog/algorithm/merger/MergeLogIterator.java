
package py.datanode.segment.datalog.algorithm.merger;

import java.util.Set;
import py.common.struct.Pair;
import py.datanode.segment.datalog.algorithm.DataLog;
import py.datanode.service.io.read.merge.MergeFailedException;
import py.instance.InstanceId;

public interface MergeLogIterator {
  boolean hasNext();

  Pair<DataLog, Set<InstanceId>> next() throws MergeFailedException;

}
