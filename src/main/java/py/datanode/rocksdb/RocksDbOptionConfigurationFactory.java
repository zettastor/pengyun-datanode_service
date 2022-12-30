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

package py.datanode.rocksdb;

import py.datanode.configuration.DataNodeConfiguration;
import py.datanode.configuration.LogPersistRocksDbConfiguration;
import py.third.rocksdb.RocksDbOptionConfiguration;

public class RocksDbOptionConfigurationFactory {
  public static RocksDbOptionConfiguration generate(
      LogPersistRocksDbConfiguration logPersistRocksDbConfiguration) {
    RocksDbOptionConfiguration configuration = new RocksDbOptionConfiguration(
        logPersistRocksDbConfiguration.getRocksDbPathConfig().getLogPersistRocksDbPath());
    configuration.setDbRootPath(
        logPersistRocksDbConfiguration.getRocksDbPathConfig().getLogPersistRocksDbPath());
    configuration
        .setMaxBackgroundCompactions(logPersistRocksDbConfiguration.getLogMaxBgCompactions());
    configuration.setMaxBackgroundFlushes(logPersistRocksDbConfiguration.getLogMaxBgFlushes());
    configuration.setBytesPerSync(logPersistRocksDbConfiguration.getLogBytesPerSync());
    configuration.setMaxTotalWalSize(logPersistRocksDbConfiguration.getLogMaxTotalWalSize());
    configuration.setBlockSize(logPersistRocksDbConfiguration.getLogBlockSize());
    configuration.setBlockCacheSizePerColumnFamily(
        logPersistRocksDbConfiguration.getLogBlockCacheSizePerColumn());
    configuration.setWriteBufferSizePerColumnFamily(
        logPersistRocksDbConfiguration.getLogWriteBufferSizePerColumn());
    configuration.setEnableCacheIndexAndFilterBlocks(
        logPersistRocksDbConfiguration.isLogEnableCacheIndexAndFilterBlocks());
    configuration.setEnablePinL0FilterAndIndexBlocksInCache(
        logPersistRocksDbConfiguration.isLogEnablePinL0FilterAndIndexBlocksInCache());
    configuration.setLevelCompactionDynamicLevelBytes(
        logPersistRocksDbConfiguration.isLogLevelCompactionDynamicLevelBytes());
    configuration.setCompressionType(logPersistRocksDbConfiguration.getLogCompressionType());
    configuration.setSyncFlag(logPersistRocksDbConfiguration.getRocksDbSyncFlag());
    configuration.setMaxNumCacheAsync(logPersistRocksDbConfiguration.getMaxNumCacheAsync());
    configuration.setMaxLogFileSize(logPersistRocksDbConfiguration.getMaxLogFileSize());
    configuration.setKeepLogFileNumber(logPersistRocksDbConfiguration.getKeepLogFileNumber());
    configuration.setLogLevel(logPersistRocksDbConfiguration.getRocksdbLogLevel());

    return configuration;
  }
}
