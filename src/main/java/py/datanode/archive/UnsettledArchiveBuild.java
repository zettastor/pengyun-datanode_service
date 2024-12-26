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

package py.datanode.archive;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.AbstractArchiveBuilder;
import py.archive.Archive;
import py.archive.ArchiveMetadata;
import py.archive.ArchiveStatus;
import py.archive.ArchiveType;
import py.archive.UnsettledArchiveMetadata;
import py.datanode.configuration.DataNodeConfiguration;
import py.exception.ArchiveTypeMismatchException;
import py.exception.ChecksumMismatchedException;
import py.exception.StorageException;
import py.storage.Storage;

public class UnsettledArchiveBuild extends AbstractArchiveBuilder {
  private static final Logger logger = LoggerFactory.getLogger(UnsettledArchiveBuild.class);
  private final DataNodeConfiguration cfg;

  public UnsettledArchiveBuild(Storage storage, DataNodeConfiguration cfg) {
    super(ArchiveType.UNSETTLED_DISK, storage);
    this.cfg = cfg;
  }

  @Override
  public Archive build()
      throws StorageException, IOException, ChecksumMismatchedException, Exception {
    UnsettledArchiveMetadata unsettledArchiveMetadata = null;
    try {
      unsettledArchiveMetadata = (UnsettledArchiveMetadata) loadArchiveMetadata();
    } catch (ArchiveTypeMismatchException e) {
      boolean iserror = true;
      if ((firstTimeStart && overwrite) || forceInitBuild) {
        iserror = false;
      }
      if (iserror) {
        logger.warn(" load archive  catch the error ", e);
        throw e;
      }
    }

    UnsettledArchive unsettledArchive = null;
    if (unsettledArchiveMetadata == null || (unsettledArchiveMetadata != null && firstTimeStart
        && overwrite)) {
      ArchiveMetadata archiveMetadata = generateArchiveMetadata();

      archiveMetadata.setStatus(ArchiveStatus.OFFLINED);
      unsettledArchiveMetadata = new UnsettledArchiveMetadata(archiveMetadata);
      unsettledArchiveMetadata.setPageSize(cfg.getPageSize());
      unsettledArchiveMetadata.setLogicalSpace(storage.size());
      unsettledArchive = new UnsettledArchive(storage, unsettledArchiveMetadata);
    } else {
      if (justloadingExistArchive) {
        logger.warn("do not need recover archive{}", unsettledArchive);
        return new UnsettledArchive(storage, unsettledArchiveMetadata);
      } else {
        if (!unsettledArchiveMetadata.getSerialNumber().equals(serialNumber)) {
          logger.warn(
              "serial number is not equal: disk serial number %s, serial number from command %s",
              unsettledArchiveMetadata.getSerialNumber(), serialNumber);

          if (unsettledArchiveMetadata.getDeviceName()
              .equalsIgnoreCase(unsettledArchiveMetadata.getSerialNumber())) {
            unsettledArchiveMetadata.setSerialNumber(serialNumber);
            logger.warn("this is only use for virsh machine ,serialnumber = devname");
          } else {
            Validate.isTrue(false,
                "serial number is not equal: disk serial number %s, serial number from command %s",
                unsettledArchiveMetadata.getSerialNumber(), serialNumber);
          }
        }

        unsettledArchiveMetadata.setDeviceName(devName);
        unsettledArchiveMetadata.setUpdatedBy(currentUser);
        unsettledArchiveMetadata.setUpdatedTime(System.currentTimeMillis());
        unsettledArchiveMetadata.setPageSize(cfg.getPageSize());
        unsettledArchiveMetadata.setLogicalSpace(storage.size());
        unsettledArchive = new UnsettledArchive(storage, unsettledArchiveMetadata);
      }
    }

    logger.warn("write unsettled archive={} to storage", unsettledArchiveMetadata);
    unsettledArchive.persistMetadata();
    return unsettledArchive;
  }

  @Override
  protected ArchiveMetadata instantiate(byte[] buffer, int offset, int length)
      throws ChecksumMismatchedException, IOException {
    ObjectMapper mapper = new ObjectMapper();
    UnsettledArchiveMetadata archiveMetadata = mapper
        .readValue(buffer, offset, length, UnsettledArchiveMetadata.class);
    return archiveMetadata;
  }
}
