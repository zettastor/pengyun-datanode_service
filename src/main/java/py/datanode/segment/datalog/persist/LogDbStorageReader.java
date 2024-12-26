/**
* Copyright (C) 2013-2024 Nanjing Pengyun Network Technology Co., Ltd.
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
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
