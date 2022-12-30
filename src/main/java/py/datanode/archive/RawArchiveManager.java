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
import java.util.List;
import py.archive.ArchiveStatusChangeFinishListener;
import py.archive.PluginPlugoutManager;
import py.archive.disklog.DiskErrorLogManager;
import py.archive.segment.SegId;
import py.archive.segment.SegmentUnitStatus;
import py.archive.segment.SegmentUnitType;
import py.archive.segment.recurring.SegmentUnitTaskExecutor;
import py.datanode.configuration.LogPersistingConfiguration;
import py.datanode.exception.MultipleInstanceIdException;
import py.datanode.segment.SegmentUnit;
import py.datanode.segment.datalog.MutationLogManager;
import py.datanode.segment.datalog.persist.LogPersister;
import py.exception.AllSegmentUnitDeadInArchviveException;
import py.exception.ArchiveIsNotCleanedException;
import py.exception.SegmentUnitBecomeBrokenException;
import py.exception.SegmentUnitRecoverFromDeletingFailException;
import py.exception.StorageException;
import py.instance.Group;
import py.instance.InstanceId;
import py.membership.SegmentMembership;
import py.thrift.share.SetArchiveConfigRequest;
import py.volume.VolumeType;

public interface RawArchiveManager extends PluginPlugoutManager {
  List<RawArchive> getRawArchives();

  RawArchive getRawArchive(long archiveId);

  SegmentUnit createSegmentUnit(SegId segId, SegmentMembership membership, int segmentWrapSize,
      VolumeType volumeType,
      Long storagePoolId, SegmentUnitType segmentUnitType,
      String volumeMetadataJson,
      String accountMetadataJson,  boolean isSecondaryCandidate,
      InstanceId replacee) throws  Exception;

  SegmentUnit createSegmentUnit(SegId segId, SegmentMembership membership, int segmentWrapSize,
      VolumeType volumeType, Long storagePoolId, SegmentUnitType segmentUnitType,
      String volumeMetadataJson,
      String accountMetadataJson, long srcVolumeId,
      SegmentMembership srcMembership,  boolean isSecondaryCandidate,
      InstanceId replacee)
      throws  Exception;

  void addSegmentUnitToEngines(SegmentUnit segUnit) throws Exception;

  SegmentUnit removeSegmentUnit(SegId segId, boolean removeFromSegmentUnitManager) throws Exception;

  SegmentUnit removeSegmentUnit(SegId segId) throws Exception;

  void recycleSegmentUnitFromDeleting(SegId segId)
      throws SegmentUnitRecoverFromDeletingFailException;

  boolean updateSegmentUnitMetadata(SegId segId, String volumeMetadataJson) throws Exception;

  boolean updateSegmentUnitMetadata(SegId segId, SegmentMembership membership,
      SegmentUnitStatus status) throws Exception;

  boolean updateSegmentUnitMetadata(SegId segId, SegmentMembership membership,
      SegmentUnitStatus status, String volumeMetadataJson) throws Exception;

  boolean updateSegmentUnitMetadata(SegId segId, SegmentMembership membership,
      SegmentUnitStatus status, boolean acceptStaleMembership) throws Exception;

  boolean updateSegmentUnitMetadata(SegId segId, SegmentMembership membership,
      SegmentUnitStatus newStatus, boolean acceptStaleMembership, String volumeMetadataJson,
      boolean persistAnyway) throws Exception;

  boolean asyncUpdateSegmentUnitMetadata(SegId segId, SegmentMembership membership,
      SegmentUnitStatus newStatus) throws Exception;

  boolean asyncUpdateSegmentUnitMetadata(SegId segId, SegmentMembership membership,
      SegmentUnitStatus newStatus, String volumeMetadataJson) throws Exception;

  long getLargestContiguousFreeSpace();

  long getTotalLogicalFreeSpace();

  /**
   * returns the total capacity of this instance.
   */
  long getTotalPhysicalCapacity();

  long getTotalLogicalSpace();

  DiskErrorLogManager getDiskLog();

  RawArchive removeArchive(long archiveId, boolean needCloseThreadPoolWithStorage)
      throws ArchiveIsNotCleanedException;

  InstanceId getInstanceIdOnArchives() throws MultipleInstanceIdException;

  void close();

  void setCatchupLogEngine(SegmentUnitTaskExecutor catchupLogEngine);

  void setStateProcessingEngine(SegmentUnitTaskExecutor stateProcessingEngine);

  void setGroup(Group group) throws Exception;

  void addArchiveStatusChangeFinishListener(ArchiveStatusChangeFinishListener listener);

  void clearArchiveStatusChangeFinishListener();

  void setMutationLogManager(MutationLogManager mutationLogManager);

  void setLogPersister(LogPersister logPersister);

  void recoverBitmap(LogPersistingConfiguration dataNodeLogPersistingCfg);

  void setArchiveConfig(long archiveId, SetArchiveConfigRequest request)
      throws JsonProcessingException, StorageException;

  void setStoragePool(RawArchive archive, Long storagePoolId)
      throws JsonProcessingException, StorageException;

  void freeArchiveWithCheck(RawArchive archive)
      throws AllSegmentUnitDeadInArchviveException, SegmentUnitBecomeBrokenException;

  void freeArchiveWithOutCheck(RawArchive archive)
      throws AllSegmentUnitDeadInArchviveException, SegmentUnitBecomeBrokenException;

}
