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

package py.datanode.segment.membership.statemachine.processors;

import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.segment.MigrationStatus;
import py.archive.segment.SegId;
import py.archive.segment.SegmentUnitMetadata;
import py.archive.segment.recurring.SegmentUnitTaskExecutor;
import py.datanode.exception.LogIdTooSmall;
import py.datanode.segment.SegmentUnit;
import py.datanode.segment.SegmentUnitCanDeletingCheck;
import py.datanode.segment.datalog.MutationLogEntry;
import py.datanode.segment.datalog.MutationLogManager;
import py.datanode.segment.datalog.SegmentLogMetadata;
import py.datanode.segment.datalog.persist.LogPersister;
import py.instance.InstanceId;
import py.membership.SegmentMembership;
import py.third.rocksdb.KvStoreException;
import py.volume.VolumeType;

/**
 * This process assists the state processor to deal with migration issue.
 *
 */
public class MigrationUtils {
  private static final Logger logger = LoggerFactory.getLogger(MigrationUtils.class);

  public static boolean stopAndRemoveResourceAfterMigrationFail(SegId segId,
      SegmentUnit segmentUnit,
      MutationLogManager mutationLogManager, LogPersister logPersister,
      SegmentUnitTaskExecutor catchupLogEngine) {
    SegmentUnitMetadata unitMetadata = segmentUnit.getSegmentUnitMetadata();
    MigrationStatus status = unitMetadata.getMigrationStatus();
    if (!status.isMigrating()) {
      logger.info("it isn't migrating for segId: {}, status={}", segId, status);
      return true;
    }

    catchupLogEngine.pause(segId);
    segmentUnit.clearFirstWritePageIndex();
    if (segmentUnit.getSecondaryCopyPageManager() != null) {
      segmentUnit.setSecondaryCopyPageManager(null);
    }

    if (!catchupLogEngine.isAllContextsPaused(segId)) {
      logger.warn("it is failure to remove the information of segment: {}", segId);
      return false;
    }

    segmentUnit.recordFailedMigration(segId);

    unitMetadata.setMigrationStatus(status.getNextStatusFromResult(false));
    logger.warn("set migration status to FROMFAILMIGRATION {}", unitMetadata.getMigrationStatus());
    return true;
  }

  private static void clearSegmentUnit(SegId segId, SegmentUnit segmentUnit,
      MutationLogManager mutationLogManager, LogPersister logPersister,
      SegmentUnitCanDeletingCheck segmentUnitCanDeletingCheck) {
    SegmentUnitMetadata unitMetadata = segmentUnit.getSegmentUnitMetadata();
    final MigrationStatus status = unitMetadata.getMigrationStatus();

    try {
      mutationLogManager.removeSegmentLogMetadata(segId);
      SegmentLogMetadata logMetadata = mutationLogManager.createOrGetSegmentLogMetadata(segId,
          segmentUnit.getSegmentUnitMetadata().getVolumeType().getVotingQuorumSize());
      logMetadata.setSegmentUnit(segmentUnit);
      segmentUnit.setSegmentLogMetadata(logMetadata);
    } catch (Exception e) {
      logger.warn("create a new segment for replacing the old one for segId={}", segId);
    }

    try {
      unitMetadata.setLogIdOfPersistBitmap(0);
      segmentUnit.getArchive()
          .persistSegmentUnitMetaAndBitmap(segmentUnit.getSegmentUnitMetadata());
    } catch (Exception e) {
      logger.error("can not persist the bitmap and segment unit metadata for segId={}", segId);
    }

    try {
      logPersister.removeLogMetaData(segmentUnit.getSegId());
    } catch (Exception e) {
      logger.warn("remove the logs file for segId={}", segId);
    }

    try {
      logPersister.persistLog(segId, MutationLogEntry.INVALID_MUTATION_LOG);
    } catch (IOException e) {
      logger.error("can't persist logs to storage for segment unit: {}", segId);
    } catch (KvStoreException e) {
      logger.error("can't persist logs to storage for segment unit: {}", segId, e);
    } catch (LogIdTooSmall e) {
      logger.error("it is impossible to throws the execption for segId: {}", segId, e);
    }

    logger.warn("the segment={} has clear all data and logs, status={}", segId, status);

    if (status == MigrationStatus.DELETED) {
      logger
          .warn("need to delete the segment: {} when migrating failure, migration status={}", segId,
              status);
      if (!unitMetadata.isSegmentUnitDeleted() && !unitMetadata.isSegmentUnitMarkedAsDeleting()) {
        try {
          segmentUnitCanDeletingCheck.deleteSegmentUnitWithOutCheck(segId, true);
        } catch (Exception e) {
          logger.error("can not mark the segment unit as deleting when migrating unsuccessfully");
        }
      } else {
        logger.warn("someone is deleting the segment: {}", segId);
      }
    } else {
      logger.warn("there is no need to delete the segment: {} when migrating failure", segId);
    }
  }

  public static boolean explicitlyDenyNewSecondary(SegmentMembership currentMembership,
      InstanceId newMember, boolean isArbiter, VolumeType volumeType) {
    if (!currentMembership.contain(newMember)) {
      if (isArbiter) {
        return currentMembership.getArbiters().size() == volumeType.getNumArbiters();
      } else {
        return currentMembership.getWriteSecondaries().size() == volumeType.getNumSecondaries();
      }

    }

    return false;
  }

}
