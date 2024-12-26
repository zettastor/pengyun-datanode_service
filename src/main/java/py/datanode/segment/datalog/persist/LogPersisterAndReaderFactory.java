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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.datanode.configuration.LogPersistRocksDbConfiguration;
import py.datanode.configuration.LogPersistingConfiguration;
import py.datanode.segment.SegmentUnitManager;
import py.datanode.segment.datalog.MutationLogReaderWriterFactory;
import py.third.rocksdb.KvStoreException;

public class LogPersisterAndReaderFactory {
  private static final Logger logger = LoggerFactory.getLogger(LogDbPersister.class);
  private final LogPersistingConfiguration logPersistingConfiguration;
  private final LogPersistRocksDbConfiguration logPersistRocksDbConfiguration;
  private final SegmentUnitManager segmentUnitManager;
  private LogStorageSystem logStorageSystem;

  public LogPersisterAndReaderFactory(SegmentUnitManager segmentUnitManager,
      LogPersistingConfiguration logPersistingConfiguration,
      LogPersistRocksDbConfiguration logPersistRocksDbConfiguration) {
    this.segmentUnitManager = segmentUnitManager;
    this.logPersistingConfiguration = logPersistingConfiguration;
    this.logPersistRocksDbConfiguration = logPersistRocksDbConfiguration;
  }
  

  public LogStorageReader buildLogReader() throws IOException, KvStoreException {
    buildLogStorageSystem();

    LogStorageReader logStorageReader;
    if (logPersistRocksDbConfiguration.getLogsCacheUsingRocksDbFlag()) {
      logStorageReader = new LogDbStorageReader(logStorageSystem);
    } else {
      logStorageReader = new LogStorageReader(logStorageSystem);
    }

    return logStorageReader;
  }
  

  public static LogStorageReader buildLogReader(LogStorageSystem logStorageSystem)
      throws IOException {
    LogStorageReader logStorageReader;
    if (logStorageSystem instanceof LogRocksDbSystem) {
      logStorageReader = new LogDbStorageReader(logStorageSystem);
    } else {
      logStorageReader = new LogStorageReader(logStorageSystem);
    }

    return logStorageReader;
  }

  private LogStorageSystem buildLogStorageSystem() throws IOException, KvStoreException {
    if (null == logStorageSystem) {
      synchronized (this) {
        if (null == logStorageSystem) {
          logger.warn("persister log to rocks db flag is {}",
              logPersistRocksDbConfiguration.getLogsCacheUsingRocksDbFlag());
          if (logPersistRocksDbConfiguration.getLogsCacheUsingRocksDbFlag()) {
            logStorageSystem = new LogRocksDbSystem(logPersistRocksDbConfiguration,
                new MutationLogReaderWriterFactory(logPersistingConfiguration),
                segmentUnitManager);
          } else {
            logStorageSystem = new LogFileSystem(logPersistingConfiguration,
                new MutationLogReaderWriterFactory(logPersistingConfiguration));
          }
        }
      }
    }
    return logStorageSystem;
  }

  public LogPersister buildLogPersister() throws IOException, KvStoreException {
    buildLogStorageSystem();

    LogPersister logPersister;
    if (logPersistRocksDbConfiguration.getLogsCacheUsingRocksDbFlag()) {
      logPersister = new LogDbPersister(logStorageSystem, segmentUnitManager);
    } else {
      logPersister = new LogPersister(logStorageSystem, segmentUnitManager);
    }

    return logPersister;
  }
}
