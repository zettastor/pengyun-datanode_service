

package py.datanode.segment.datalog.persist;

import java.io.IOException;
import java.util.Map;
import py.archive.segment.SegId;
import py.datanode.exception.LogIdNotFoundException;
import py.third.rocksdb.KvStoreException;

public interface LogStorageSystem {
  public LogStorageMetadata getLogStorageToWrite(SegId segId);

  public LogStorageMetadata createLogStorageToWrite(SegId segId, long id,
      LogStorageMetadata originalStorageMetadata) throws IOException;

  public LogStorageMetadata createLogStorageWhenNoLogFileExist(SegId segId, long id)
      throws IOException, KvStoreException;

  public LogStorageMetadata getOrCreateLogStorageToWrite(SegId segId, long id) throws IOException;

  public LogStorageMetadata getLogStorageToRead(SegId segId, long id)
      throws IOException, LogIdNotFoundException;

  public LogStorageMetadata getLatestLogStorageToRead(SegId segId) throws IOException;

  public void removeLogStorage(SegId segId) throws IOException, KvStoreException;

  public Map<SegId, LogStorageMetadata> init() throws IOException, KvStoreException;

  public void close();

}
