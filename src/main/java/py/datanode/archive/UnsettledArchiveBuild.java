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
