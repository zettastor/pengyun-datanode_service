

package py.datanode.service.io.read.merge;

public interface LocalLogs {
  boolean isLogCommitted(long logId);

}
