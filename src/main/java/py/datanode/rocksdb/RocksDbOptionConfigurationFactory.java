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
