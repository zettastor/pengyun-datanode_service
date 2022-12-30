/*
 * Copyright (c) 2022. PengYunNetWork
 *
 * This program is free software: you can use, redistribute, and/or modify it
 * under the terms of the GNU Affero General Public License, version 3 or later ("AGPL"),
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 *  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *
 *  You should have received a copy of the GNU Affero General Public License along with
 *  this program. If not, see <http://www.gnu.org/licenses/>.
 */

package py.datanode.segment.datalog.persist;

import java.io.IOException;
import java.util.List;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.segment.SegId;
import py.datanode.exception.LogIdNotFoundException;
import py.datanode.segment.SegmentUnitManager;
import py.datanode.segment.datalog.MutationLogEntry;
import py.third.rocksdb.KvStoreException;

public class LogDbStorageReader extends LogStorageReader {
  private static final Logger logger = LoggerFactory.getLogger(LogDbStorageReader.class);
  private LogRocksDbSystem logRocksDbSystem;
  private SegmentUnitManager segUnitManager;

  public LogDbStorageReader(LogStorageSystem logStorageSystem) throws IOException {
    super(null);
    Validate.isTrue(logStorageSystem instanceof LogRocksDbSystem,
        "log db persister init by invalid log db system");
    this.logRocksDbSystem = (LogRocksDbSystem) logStorageSystem;
  }

  @Override
  public boolean logIdExists(SegId segId, long id) throws IOException, KvStoreException {
    return logRocksDbSystem.logIdExists(segId, id);
  }

  @Override
  public List<MutationLogEntry> readLatestLogs(SegId segId, int maxNum)
      throws IOException, KvStoreException {
    return logRocksDbSystem.readLatestLogs(segId, maxNum);
  }

  @Override
  public List<MutationLogEntry> readLogsAfter(SegId segId, long id, int maxNum)
      throws IOException, LogIdNotFoundException, KvStoreException {
    return readLogsAfter(segId, id, maxNum, false);
  }

  @Override
  public List<MutationLogEntry> readLogsAfter(SegId segId, long id, int maxNum,
      boolean checkIdExists)
      throws IOException, LogIdNotFoundException, KvStoreException {
    return this.logRocksDbSystem.readLogsAfter(segId, id, maxNum, checkIdExists);
  }
}
