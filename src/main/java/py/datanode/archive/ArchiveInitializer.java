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

package py.datanode.archive;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.concurrent.CountDownLatch;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import py.archive.AbstractArchiveBuilder;
import py.archive.Archive;
import py.archive.ArchiveMetadata;
import py.archive.ArchiveStatus;
import py.archive.ArchiveType;
import py.archive.BaseArchiveBuilder;
import py.archive.StorageType;
import py.common.tlsf.bytebuffer.manager.TlsfByteBufferManagerFactory;
import py.datanode.LoggingPropertiesConfigurator;
import py.datanode.configuration.DataNodeConfiguration;
import py.datanode.configuration.DataNodeConfigurationUtils;
import py.datanode.storage.impl.PageAlignedStorageFactory;
import py.exception.StorageException;
import py.storage.Storage;
import py.storage.async.AsyncIoStat;
import py.storage.impl.FileStorageFactory;

@Configuration
@Import(DataNodeConfiguration.class)
public class ArchiveInitializer {
  private static final Logger logger = LoggerFactory.getLogger(ArchiveInitializer.class);

  public static boolean isSlowDisk(Storage storage, String storagePath, int randomThreshold,
      int sequentialThreshold) {
    logger.info("start check storage:{}", storage);
    int blockSize = 4096;
    boolean isSlowDisk = false;
    BaseArchiveBuilder build = new BaseArchiveBuilder(null, storage);
    Archive archive = null;
    try {
      archive = build.build();
    } catch (Exception e) {
      logger.warn("caught an exception when building the archive", e);
      return false;
    }

    ArchiveMetadata archiveMetadata = archive.getArchiveMetadata();
    if (archiveMetadata != null) {
      logger.info("storage:{} is not empy, don't do IOPS check.", storagePath);
      return false;
    }
    try {
      AsyncIoStat asyncIoStat = new AsyncIoStat();
      asyncIoStat.open(storagePath);
      if (asyncIoStat.randomReadChecker(blockSize, randomThreshold) || asyncIoStat
          .randomWriteChecker(blockSize, randomThreshold) || asyncIoStat
          .sequentialReadChecker(blockSize, sequentialThreshold) || asyncIoStat
          .sequentialWriteChecker(blockSize, sequentialThreshold)) {
        isSlowDisk = true;
      }
      asyncIoStat.close();
    } catch (StorageException e) {
      logger.warn("async io stat exception.", e);
      return false;
    }

    return isSlowDisk;
  }

  public static void main(String[] args) throws Exception {
    LoggingPropertiesConfigurator.configure(Level.DEBUG, "archiveinitializer.log");

    CommandLineArgs cliArgs = new CommandLineArgs();
    JCommander commander = new JCommander(cliArgs, args);
    logger.warn("enter archive initialization, parameter={}", cliArgs);

    ApplicationContext ctx = new AnnotationConfigApplicationContext(DataNodeConfiguration.class);
    DataNodeConfiguration dataNodeConfiguration = ctx.getBean(DataNodeConfiguration.class);
    if (dataNodeConfiguration == null || dataNodeConfiguration.getSegmentUnitSize() < 0
        || dataNodeConfiguration.getPageSize() < 0) {
      logger.error(
          "can't initialize archive because the storage configuration is not property. " 
              + "(Value is null or negative), System exit");
      System.exit(1);
    }

    TlsfByteBufferManagerFactory.init(512, 16 * 1024 * 1024, true);

    Initializer initializer = new Initializer(dataNodeConfiguration);

    String cmd =
        "ls -l" + " " + cliArgs.storagePath.substring(0, cliArgs.storagePath.lastIndexOf("/"));
    logger.warn("going to execute [{}], and storage path:{}", cmd, cliArgs.storagePath);
    Process currentProcess = Runtime.getRuntime().exec(cmd);

    CountDownLatch waitLatch = new CountDownLatch(1);
    new Thread(() -> {
      InputStream is2 = currentProcess.getInputStream();
      BufferedReader br2 = new BufferedReader(new InputStreamReader(is2));
      try {
        String line1;
        while ((line1 = br2.readLine()) != null) {
          if (line1 != null) {
            logger.warn("before init storage, output: {}", line1);
          }
        }
      } catch (IOException e) {
        logger.warn("read from error stream", e);
      } finally {
        try {
          is2.close();
        } catch (IOException e) {
          logger.error("", e);
        }
        waitLatch.countDown();
      }
    }).start();
    waitLatch.await();

    FileStorageFactory storageFactory = new PageAlignedStorageFactory();
    Storage storage = null;
    try {
      storage = storageFactory.setFile(new File(cliArgs.storagePath))
          .generate(cliArgs.storagePath);
      boolean slowDisk = false;
      if (!dataNodeConfiguration.getArchiveConfiguration().isOnVms() && dataNodeConfiguration
          .isArchiveInitSlowDiskCheckerEnabled()) {
        logger.info("disk threshold random:{} sequential:{}",
            dataNodeConfiguration.getArchiveInitDiskThresholdRandom(),
            dataNodeConfiguration.getArchiveInitDiskThresholdSequential());
        slowDisk = isSlowDisk(storage, cliArgs.storagePath,
            dataNodeConfiguration.getArchiveInitDiskThresholdRandom(),
            dataNodeConfiguration.getArchiveInitDiskThresholdSequential());
      }
      AbstractArchiveBuilder builder = initializer.getArchiveBuilder(cliArgs, storage);
      Archive archive = builder.build();

      if (slowDisk) {
        logger.warn("find slow disk:{}", cliArgs.storagePath);
        archive.getArchiveMetadata().setStatus(ArchiveStatus.DEGRADED);
        archive.persistMetadata();
        logger.warn("persist metadata into disk:{}", archive.getArchiveMetadata());
      }
      logger.warn("archive={}", archive);
    } catch (Exception e) {
      logger.error("can not build the archive={} when initializing", cliArgs, e);
    } finally {
      if (storage != null) {
        storage.close();
      }
    }
  }

  private static class CommandLineArgs {
    public static final String STORAGE = "--storage";
    public static final String FIRST_TIME_START = "--firstTimeStart";
    public static final String STORAGE_TYPE = "--storageType";
    public static final String DEV_NAME = "--devName";
    public static final String UPDATED_BY = "--updatedBy";
    public static final String SERIAL_NUMBER = "--serialNumber";
    public static final String RUN_IN_REALTIME = "--runInRealTime";
    public static final String ARCHIVEID = "--archiveId";
    public static final String INSTANCEID = "--instanceId";
    public static final String GROUPID = "--groupId";
    public static final String SUB_DIR = "--subDir";
    public static final String FORCE_INIT_BUILD = "--forceInitBuild";
    public static final String FILE_SYSTEM_PARTITION_NAME = "--fileSystemPartitionName";
    @Parameter(names = STORAGE, description = "Path to the file where a storage will be created. " 
        + "Required", required = true)
    public String storagePath;
    @Parameter(names = FIRST_TIME_START, description = "<true|false>. " 
        + "specifies whether data node service start for first time. Optional")
    public String firstTimeStart;
    @Parameter(names = STORAGE_TYPE, description = "<SSD|SATA>. " 
        + "specifies disk type", required = true)
    public String storageType;
    @Parameter(names = DEV_NAME, description = "specifies disk name, " 
        + "such as /dev/sdc", required = true)
    public String devName;
    @Parameter(names = UPDATED_BY, description = "specifies who updates the storage metadata. " 
        + "Optional")
    public String updatedBy;
    @Parameter(names = SERIAL_NUMBER, description = "serial number from disk, " 
        + "it is unique for the disk.", required = true)
    public String serialNumber;
    @Parameter(names = RUN_IN_REALTIME, description = "Mark called In runtime or initialization", 
        required = true)
    public String runInRealTime;
    @Parameter(names = ARCHIVEID, description = "specifies identifier for the archive. " 
        + "Optional")
    public String archiveId;
    @Parameter(names = INSTANCEID, description = "specifies the id of datanode which " 
        + "archive is belong to. Optional")
    public String instanceId;
    @Parameter(names = GROUPID, description = "specifies the group id which " 
        + "datanode should join in. Optional")
    public String groupId;
    @Parameter(names = SUB_DIR, description = "specifies the application of the archive.",
        required = true)
    public String subDir;
    @Parameter(names = FORCE_INIT_BUILD, description = "<true|false>. specifies whether" 
        + " force init archive. Optional")
    public String forceInitBuild;
    @Parameter(names = FILE_SYSTEM_PARTITION_NAME, description = "file system partition name, " 
        + "which indicate partition mount point name.")
    public String fileSystemPartitionName;

    @Parameter(names = "--help", help = true)
    private boolean help;

    @Override
    public String toString() {
      return "CommandLineArgs{"
          + "storagePath='" + storagePath + '\''
          + ", firstTimeStart='" + firstTimeStart + '\''
          + ", storageType='" + storageType + '\''
          + ", devName='" + devName + '\''
          + ", updatedBy='" + updatedBy + '\''
          + ", serialNumber='" + serialNumber + '\''
          + ", runInRealTime='" + runInRealTime + '\''
          + ", archiveId='" + archiveId + '\''
          + ", instanceId='" + instanceId + '\''
          + ", groupId='" + groupId + '\''
          + ", subDir='" + subDir + '\''
          + ", forceInitBuild='" + forceInitBuild + '\''
          + ", fileSystemPartitionName='" + fileSystemPartitionName + '\''
          + ", help=" + help
          + '}';
    }
  }

  public static class Initializer {
    private DataNodeConfiguration dataNodeConfiguration;

    public Initializer(DataNodeConfiguration dataNodeConfiguration) {
      this.dataNodeConfiguration = dataNodeConfiguration;
    }

    public AbstractArchiveBuilder getArchiveBuilder(CommandLineArgs cliArgs, Storage storage)
        throws Exception {
      ArchiveType archiveType = DataNodeConfigurationUtils
          .getArchiveTypeByDirName(dataNodeConfiguration, cliArgs.subDir);

      AbstractArchiveBuilder builder = ArchiveUtils
          .getArchiveBuilder(dataNodeConfiguration, storage, archiveType);
      builder.setJustloadingExistArchive(false);

      builder.setFirstTimeStart(cliArgs.firstTimeStart.equalsIgnoreCase("true"));

      builder.setRunInRealTime(cliArgs.runInRealTime.equals("true"));
      builder.setArchiveType(archiveType);
      builder.setDevName(cliArgs.devName);
      builder.setStorageType(StorageType.valueOf(cliArgs.storageType.toUpperCase()));
      builder.setStoragePath(cliArgs.storagePath);
      builder.setSerialNumber(cliArgs.serialNumber);
      builder.setForceInitBuild(StringUtils.isEmpty(cliArgs.forceInitBuild) ? false
          : StringUtils.equalsIgnoreCase(cliArgs.forceInitBuild, "true"));
      String user = System.getProperty("user.name");
      builder.setCurrentUser(user != null ? user : "unknown");
      builder.setFileSystemPartitionName(StringUtils.isEmpty(cliArgs.fileSystemPartitionName) ? null
          : cliArgs.fileSystemPartitionName);

      String mode = dataNodeConfiguration.getArchiveInitMode();
      if (!mode.equalsIgnoreCase("overwrite") && !mode.equalsIgnoreCase("append")) {
        logger.error("archive initialization fail.Current mode is {}.", mode);
        System.exit(1);
      }
      builder.setOverwrite(mode.equalsIgnoreCase("overwrite"));

      logger.warn("initialization archive factory={}", builder);
      return builder;
    }
  }
}
