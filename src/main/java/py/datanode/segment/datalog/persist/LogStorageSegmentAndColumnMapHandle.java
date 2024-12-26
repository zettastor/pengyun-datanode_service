

package py.datanode.segment.datalog.persist;

import org.rocksdb.RocksDB;
import py.third.rocksdb.RocksDbOptionConfiguration;
import py.third.rocksdb.TemplateRocksDbColumnFamilyHandle;

public class LogStorageSegmentAndColumnMapHandle extends
    TemplateRocksDbColumnFamilyHandle<LogStorageSegmentAndColumnMapKey,
        LogStorageSegmentAndColumnMapValue> {
  LogStorageSegmentAndColumnMapHandle(String path) {
    super(new RocksDbOptionConfiguration(path));
  }

  @Override
  protected boolean needCacheRecordsNumInMemory() {
    return false;
  }

  @Override
  protected String packColumnFamilyName() {
    return String.valueOf(RocksDB.DEFAULT_COLUMN_FAMILY);
  }
}
