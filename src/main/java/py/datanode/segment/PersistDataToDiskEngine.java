

package py.datanode.segment;

import java.util.concurrent.RejectedExecutionException;

public interface PersistDataToDiskEngine {
  public void asyncPersistData(PersistDataContext context) throws RejectedExecutionException;

  /**
   * Persist the data right away. No rejected exception will be thrown.
   *
   */
  public void syncPersistData(PersistDataContext context);

  public void start() throws Exception;

  public void stop() throws Exception;
}
