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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.ByteBuffer;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.Archive;
import py.archive.ArchiveMetadata;
import py.archive.ArchiveStatus;
import py.archive.ArchiveStatusListener;
import py.archive.ArchiveType;
import py.archive.UnsettledArchiveMetadata;
import py.common.tlsf.bytebuffer.manager.TlsfByteBufferManager;
import py.common.tlsf.bytebuffer.manager.TlsfByteBufferManagerFactory;
import py.exception.ArchiveNotExistException;
import py.exception.ArchiveStatusException;
import py.exception.StorageException;
import py.storage.Storage;

public class UnsettledArchive extends Archive {
  private static final Logger logger = LoggerFactory.getLogger(UnsettledArchive.class);
  private final UnsettledArchiveMetadata unsettledArchiveMetadata;

  public UnsettledArchive(Storage storage, UnsettledArchiveMetadata unsettledArchiveMetadata) {
    super(storage);
    this.unsettledArchiveMetadata = unsettledArchiveMetadata;
  }

  @Override
  public void addListener(ArchiveStatusListener listener) {
    boolean foundListener = false;
    for (ArchiveStatusListener listenerExist : archiveStatusListers) {
      if (listenerExist == listener) {
        foundListener = true;
      }
    }
    if (!foundListener) {
      this.archiveStatusListers.add(listener);
    }
  }

  @Override
  public void clearArchiveStatusListener() {
    this.archiveStatusListers.clear();
  }

  @Override
  public ArchiveMetadata getArchiveMetadata() {
    return unsettledArchiveMetadata;
  }

  public UnsettledArchiveMetadata getUnsettledArchiveMetadata() {
    return unsettledArchiveMetadata;
  }

  @Override
  public void persistMetadata() throws JsonProcessingException, StorageException {
    ObjectMapper objectMapper = new ObjectMapper();
    byte[] archiveMetadataBuf = objectMapper.writeValueAsBytes(unsettledArchiveMetadata);
    int bufLen = archiveMetadataBuf.length + 8 + 4 + 8;
    ByteBuffer srcBuffer = ByteBuffer.allocate(bufLen);
    ArchiveType archiveType = unsettledArchiveMetadata.getArchiveType();
    srcBuffer.putLong(archiveType.getMagicNumber());
    srcBuffer.putInt(archiveMetadataBuf.length);
    srcBuffer.put(archiveMetadataBuf);
   
    srcBuffer.putLong(0);
    srcBuffer.clear();

    TlsfByteBufferManager tlsfByteBufferManager = TlsfByteBufferManagerFactory.instance();
    Validate.notNull(tlsfByteBufferManager);

    ByteBuffer tempPagedAlignedBuffer = tlsfByteBufferManager
        .blockingAllocate(archiveType.getArchiveHeaderLength());

    try {
      tempPagedAlignedBuffer.put(srcBuffer.array()).clear();
      storage.write(archiveType.getArchiveHeaderOffset(), tempPagedAlignedBuffer);
      logger.debug("write unsettled archive metadata={}", unsettledArchiveMetadata);
    } finally {
      tlsfByteBufferManager.release(tempPagedAlignedBuffer);
    }
  }

  @Override
  public void setArchiveStatus(ArchiveStatus newArchiveStatus) throws ArchiveStatusException,
      ArchiveNotExistException {
    Validate.notNull(this.getArchiveMetadata());
    ArchiveStatus currentStatus = this.getArchiveMetadata().getStatus();
    if (currentStatus == newArchiveStatus) {
      logger.warn("{}set same archive status {}", this.getArchiveMetadata(), newArchiveStatus);
      return;
    }

    logger.warn("set archive with new status {}, archive is {}", newArchiveStatus,
        this.getArchiveMetadata());

    if (archiveStatusListers.size() == 0) {
      logger.warn("there is no listener for archive={}", getArchiveMetadata());
      Validate.isTrue(false);
    }

    currentStatus.validate(newArchiveStatus);
    try {
      for (ArchiveStatusListener lister : archiveStatusListers) {
        switch (newArchiveStatus) {
          case GOOD:
            lister.becomeGood(this);
            break;
          case OFFLINED:
            lister.becomeOfflined(this);
            break;
          case INPROPERLY_EJECTED:
            lister.becomeInProperlyEjected(this);
            break;
          case EJECTED:
            lister.becomeEjected(this);
            break;
          case OFFLINING:
          case DEGRADED:
          case BROKEN:
          case CONFIG_MISMATCH:

            throw new ArchiveStatusException("this is unsettled Archive");
          default:
            break;
        }
      }
    } catch (ArchiveNotExistException ne) {
      logger.error("archive not found in manager ", this.getArchiveMetadata(), ne);
      throw new ArchiveNotExistException("change status error");
    } catch (Exception e) {
      logger.error("change archive {} to status {} error ", this.getArchiveMetadata(),
          newArchiveStatus, e);
      throw new ArchiveStatusException("change status error");
    }
  }

  @Override
  public String toString() {
    return "UnsettledArchive{" + "unsettledArchiveMetadata=" + unsettledArchiveMetadata + '}';
  }
}
