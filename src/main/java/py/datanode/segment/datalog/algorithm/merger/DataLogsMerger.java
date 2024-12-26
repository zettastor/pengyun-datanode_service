
package py.datanode.segment.datalog.algorithm.merger;

import py.datanode.segment.datalog.algorithm.DataLog;
import py.instance.InstanceId;

public interface DataLogsMerger {
  void addLog(InstanceId instance, DataLog log);

  MergeLogIterator iterator(long maxLogIdToMerge);

}
