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
