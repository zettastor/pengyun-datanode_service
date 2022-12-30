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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.Archive;
import py.archive.ArchiveStatus;
import py.archive.ArchiveStatusChangeFinishListener;
import py.archive.ArchiveStatusListener;
import py.archive.ArchiveType;
import py.datanode.configuration.DataNodeConfiguration;
import py.datanode.page.impl.AbstractPlugInPlugOutManager;
import py.exception.ArchiveIsNotCleanedException;
import py.exception.ArchiveNotExistException;
import py.exception.ArchiveStatusException;
import py.exception.ArchiveTypeNotSupportException;
import py.exception.InvalidInputException;
import py.exception.JnotifyAddListerException;
import py.exception.StorageException;
import py.instance.InstanceId;
import py.netty.exception.TimeoutException;
import py.storage.StorageExceptionHandlerChain;
import py.thrift.share.ArchiveIsUsingExceptionThrift;

public class UnsettledArchiveManagerImpl extends AbstractPlugInPlugOutManager
    implements ArchiveStatusListener, UnsettledArchiveManager {
  private static final Logger logger = LoggerFactory.getLogger(UnsettledArchiveManagerImpl.class);

  private final List<ArchiveStatusChangeFinishListener> archiveStatusReportListers = 
      new CopyOnWriteArrayList<>();
  DataNodeConfiguration cfg;
  private InstanceId instanceId;

  public UnsettledArchiveManagerImpl(InstanceId instanceId, DataNodeConfiguration cfg,
      StorageExceptionHandlerChain storageExceptionHandlerChain, List<UnsettledArchive> archives) {
    this.instanceId = instanceId;
    this.cfg = cfg;
    for (UnsettledArchive archive : archives) {
      mapArchiveIdToArchive.put(archive.getArchiveMetadata().getArchiveId(), archive);
    }
  }

  @Override
  public void becomeGood(Archive archive)
      throws ArchiveNotExistException, JsonProcessingException, StorageException, 
      InterruptedException, TimeoutException, ArchiveStatusException {
  }

  @Override
  public void becomeOfflining(Archive archive)
      throws ArchiveNotExistException, JsonProcessingException, StorageException, 
      ArchiveStatusException {
    throw new ArchiveStatusException("this is a unsettled Archive");
  }

  @Override
  public void becomeOfflined(Archive archive)
      throws ArchiveNotExistException, JsonProcessingException, StorageException,
      ArchiveStatusException, ArchiveIsNotCleanedException {
    archive.getArchiveMetadata().setStatus(ArchiveStatus.OFFLINED);
  }

  @Override
  public void becomeEjected(Archive archive)
      throws ArchiveNotExistException, ArchiveStatusException {
    UnsettledArchive unsettledArchive = (UnsettledArchive) this
        .getArchive(archive.getArchiveMetadata().getArchiveId());
    if (unsettledArchive == null) {
      throw new ArchiveNotExistException("archive: " + archive + "does not exist!");
    }

    final ArchiveStatus oldArchiveStatus = unsettledArchive.getArchiveMetadata().getStatus();
    unsettledArchive.getArchiveMetadata().setStatus(ArchiveStatus.EJECTED);

    unsettledArchive.getArchiveMetadata().setUpdatedTime(System.currentTimeMillis());

    for (ArchiveStatusChangeFinishListener listener : archiveStatusReportListers) {
      listener.becomeEjected(unsettledArchive, oldArchiveStatus);
    }
  }

  @Override
  public void becomeConfigMismatch(Archive archive)
      throws ArchiveNotExistException, ArchiveStatusException {
    throw new ArchiveStatusException("this is a unsettled Archive");
  }

  @Override
  public void becomeDegrade(Archive archive)
      throws ArchiveNotExistException, ArchiveStatusException {
    throw new ArchiveStatusException("this is a unsettled Archive");
  }

  @Override
  public void becomeInProperlyEjected(Archive archive)
      throws ArchiveNotExistException, ArchiveStatusException {
    UnsettledArchive unsettledArchive = (UnsettledArchive) this
        .getArchive(archive.getArchiveMetadata().getArchiveId());
    if (unsettledArchive == null) {
      throw new ArchiveNotExistException("archive: " + archive + "does not exist!");
    }

    final ArchiveStatus oldArchiveStatus = unsettledArchive.getArchiveMetadata().getStatus();
    unsettledArchive.getArchiveMetadata().setStatus(ArchiveStatus.INPROPERLY_EJECTED);

    unsettledArchive.getArchiveMetadata().setUpdatedTime(System.currentTimeMillis());

    for (ArchiveStatusChangeFinishListener listener : archiveStatusReportListers) {
      listener.becomeInProperlyEjected(unsettledArchive, oldArchiveStatus);
    }
  }

  @Override
  public void becomeBroken(Archive archive)
      throws ArchiveNotExistException, ArchiveStatusException {
    throw new ArchiveStatusException("this is a unsettled Archive");
  }

  @Override
  public void removeArchive(long archiveId) {
    UnsettledArchive archive = (UnsettledArchive) mapArchiveIdToArchive.get(archiveId);
    if (archive == null) {
      return;
    }

    mapArchiveIdToArchive.remove(archiveId);
  }

  @Override
  public List<Archive> getArchives() {
    List<Archive> allArchive = new ArrayList<>();
    for (Archive archive : mapArchiveIdToArchive.values()) {
      allArchive.add(archive);
    }
    return allArchive;
  }

  @Override
  public void plugin(Archive archive)
      throws ArchiveTypeNotSupportException, InvalidInputException, ArchiveIsUsingExceptionThrift,
      ArchiveStatusException, JnotifyAddListerException, ArchiveIsNotCleanedException, 
      StorageException {
    logger.warn("some one plug in the archive {}", archive);
    if (!(archive instanceof UnsettledArchive)) {
      throw new ArchiveTypeNotSupportException("can not plug in an not wts archive");
    }

    if (archive.getArchiveMetadata().getArchiveType() != ArchiveType.UNSETTLED_DISK) {
      throw new ArchiveTypeNotSupportException("can not plug in an not wts archive");
    }

    archive.clearArchiveStatusListener();
    archive.addListener(this);

    super.plugin(archive);
    try {
      this.getArchive(archive.getArchiveMetadata().getArchiveId())
          .setArchiveStatus(ArchiveStatus.OFFLINED);
    } catch (ArchiveNotExistException e) {
      Validate.isTrue(false);
    }
  }
}
