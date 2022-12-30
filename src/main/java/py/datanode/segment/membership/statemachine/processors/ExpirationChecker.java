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

import static py.archive.segment.SegmentUnitBitmap.SegmentUnitBitMapType.Migration;

import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.segment.CloneStatus;
import py.archive.segment.CloneType;
import py.archive.segment.SegmentUnitMetadata;
import py.archive.segment.SegmentUnitStatus;
import py.common.struct.Pair;
import py.datanode.segment.SegmentUnit;
import py.datanode.segment.datalog.SegmentLogMetadata;
import py.datanode.segment.membership.statemachine.StateProcessingResult;
import py.datanode.segment.membership.statemachine.StateProcessor;
import py.datanode.service.worker.MembershipPusher;
import py.instance.InstanceId;
import py.membership.SegmentMembership;
import py.thrift.datanode.service.HeartbeatResultThrift;

public class ExpirationChecker extends StateProcessor {
  private static final Logger logger = LoggerFactory.getLogger(ExpirationChecker.class);

  private static final long DEFAULT_FIXED_DELAY = 1500;

  public ExpirationChecker(StateProcessingContext context) {
    super(context);
  }

  @Override
  public StateProcessingResult process(StateProcessingContext context) {
    SegmentUnit segmentUnit = context.getSegmentUnit();
    if (segmentUnit == null || segmentUnit.isDataNodeServiceShutdown()) {
      logger.warn(" segment unit {} not exist or data node service shutdown {}", context.getSegId(),
          appContext.getInstanceId());
      return getFailureResultWithRandomizedDelay(context, context.getStatus(), DEFAULT_FIXED_DELAY);
    }

    SegmentUnitMetadata metadata = segmentUnit.getSegmentUnitMetadata();
    SegmentUnitStatus currentStatus = metadata.getStatus();

    logger.debug("checking expiration: {} @ segId:[{}]", context, segmentUnit);

    if (isSecondaryNeedCheck(segmentUnit)) {
      segmentUnit.lockStatus();
      boolean persistForce = false;
      try {
        if (segmentUnit.myLeaseExpired(currentStatus)) {
          logger.warn(
              "The lease of Segment unit ({} ) at {} has expired. Changing my status from {} "
                  + " to Start so that I can start the primary election process",
              appContext.getInstanceId(),
              metadata, currentStatus);

          try {
            persistForce = archiveManager.asyncUpdateSegmentUnitMetadata(context.getSegId(), null,
                SegmentUnitStatus.Start);
            return getSuccesslResultWithZeroDelay(context, SegmentUnitStatus.Start);
          } catch (Exception e) {
            logger.error(
                "Can't update segment unit {} status from {} to Start, System exit ",
                segmentUnit.getSegId(), currentStatus, e);

            System.exit(-1);

            return null;
          }
        } else {
          logger.debug("lease {} @ [{}] doesn't expire yet. Check {}ms it later ",
              segmentUnit.getMyLease(), segmentUnit.getSegId(), DEFAULT_FIXED_DELAY);
          return getSuccesslResultWithFixedDelay(context, currentStatus,
              DEFAULT_FIXED_DELAY);
        }
      } finally {
        segmentUnit.unlockStatus();
        if (persistForce) {
          segmentUnit.getArchive()
              .persistSegmentUnitMeta(segmentUnit.getSegmentUnitMetadata(), true);
        }
      }
    } else if (currentStatus == SegmentUnitStatus.Primary) {
      if (segmentUnit.isBecomingPrimary()) {
        logger.warn("the become primary process is not finished {}", segmentUnit);
        return getSuccesslResultWithFixedDelay(context, currentStatus, DEFAULT_FIXED_DELAY);
      }

      int leaseExtensionFrozenNum = 0;
      int segmentNotFoundNum = 0;
      boolean haveStaleMembership = false;

      Map<InstanceId, HeartbeatResultThrift> resultMap = segmentUnit.getHearbeatResult();
      for (Entry<InstanceId, HeartbeatResultThrift> entry : resultMap.entrySet()) {
        HeartbeatResultThrift result = entry.getValue();
        if (result == HeartbeatResultThrift.SegmentNotFound) {
          segmentNotFoundNum++;
        } else if (result == HeartbeatResultThrift.LeaseExtensionFrozen) {
          leaseExtensionFrozenNum++;
        } else if (result == HeartbeatResultThrift.StaleMembership) {
          logger.warn("segment unit {} version lower than secondary {}", segmentUnit, entry);
          haveStaleMembership = true;
          break;
        }
      }
      Pair<Boolean, Set<InstanceId>> returnPair = segmentUnit.primaryLeaseExpired();
      boolean primaryLeaseExpired = returnPair.getFirst();
      Set<InstanceId> expiredMembers = returnPair.getSecond();

      int votingQuorumSize = segmentUnit.getSegmentUnitMetadata().getVolumeType()
          .getVotingQuorumSize();
      if (haveStaleMembership || segmentNotFoundNum >= votingQuorumSize
          || leaseExtensionFrozenNum >= votingQuorumSize || primaryLeaseExpired) {
        logger.warn(
            "primary will turn to start status: {}," 
                + " haveStaleMembership {}, segmentNotFoundNum {}, " 
                + "leaseExtensionFrozenNum {}, expired {}",
            segmentUnit, haveStaleMembership, segmentNotFoundNum, leaseExtensionFrozenNum,
            primaryLeaseExpired);
        try {
          archiveManager
              .updateSegmentUnitMetadata(segmentUnit.getSegId(), null, SegmentUnitStatus.Start);
          return getFailureResultWithZeroDelay(context, SegmentUnitStatus.Start);
        } catch (Exception e) {
          logger.error("Can't update segment unit {} status from Primary to Start, System exit",
              segmentUnit.getSegId());

          System.exit(-1);

          return null;
        }
      }

      SegmentMembership newMembership = segmentUnit.getSegmentUnitMetadata().getMembership();
      if (!expiredMembers.isEmpty()) {
        boolean memberMovedOrRemoved = false;
        Set<InstanceId> expiredSecondaries = new HashSet<>();
        Set<InstanceId> expiredArbiters = new HashSet<>();
        for (InstanceId expiredMember : expiredMembers) {
          if (newMembership.getInactiveSecondaries().contains(expiredMember)) {
            logger.trace(
                "the expired secondary {} is already in" 
                    + " the set of inactive members in the membership. do nothing",
                expiredMember);
            continue;
          } else if (newMembership.isSecondary(expiredMember) || newMembership
              .isJoiningSecondary(expiredMember)) {
            newMembership = newMembership.aliveSecondaryBecomeInactive(expiredMember);
            segmentUnit.getSegmentLogMetadata()
                .setPrimaryMaxLogIdWhenPsi(segmentUnit.getSegmentLogMetadata().getMaxLogId());
            memberMovedOrRemoved = true;
            expiredSecondaries.add(expiredMember);
          } else if (newMembership.isArbiter(expiredMember)) {
            newMembership = newMembership.arbiterBecomeInactive(expiredMember);
            memberMovedOrRemoved = true;
            expiredArbiters.add(expiredMember);
          } else if (newMembership.isSecondaryCandidate(expiredMember)) {
            newMembership = newMembership.removeSecondaryCandidate(expiredMember);
            memberMovedOrRemoved = true;
            expiredSecondaries.add(expiredMember);
          }
        }

        Validate.isTrue(newMembership.getAliveSecondaries().size() >= votingQuorumSize - 1,
            "it is impossible that the alive secondaries in " + newMembership.getAliveSecondaries()
                .size()
                + " larger than voting quorum" + votingQuorumSize);

        try {
          if (memberMovedOrRemoved) {
            logger.warn("after move alive secondaries to inactive, the new membership becomes {} ",
                newMembership);
            archiveManager.updateSegmentUnitMetadata(segmentUnit.getSegId(), newMembership, null);

            for (InstanceId expiredArbiter : expiredArbiters) {
              segmentUnit.memberExpired(expiredArbiter, true);
            }
            for (InstanceId expiredSecondary : expiredSecondaries) {
              segmentUnit.memberExpired(expiredSecondary, false);
            }
          }
          if (memberMovedOrRemoved || !newMembership.isQuorumUpdated()) {
            MembershipPusher.getInstance(appContext.getInstanceId()).submit(segmentUnit.getSegId());
          }
        } catch (Exception e) {
          logger.warn(
              "when some secondaries are expired, " 
                  + "we can't update membership from {} to the new membership {} ",
              segmentUnit.getSegmentUnitMetadata().getMembership(), newMembership);
          return getFailureResultWithFixedDelay(context, currentStatus, DEFAULT_FIXED_DELAY);
        }
      } else if (!newMembership.isQuorumUpdated()) {
        MembershipPusher.getInstance(appContext.getInstanceId()).submit(segmentUnit.getSegId());
      }

      return getSuccesslResultWithFixedDelay(context, currentStatus, DEFAULT_FIXED_DELAY);
    } else {
      logger.debug("the current status {} is not concerned by lease expiration", currentStatus);
      return getSuccesslResultWithZeroDelay(context, currentStatus);
    }
  }

  private boolean isSecondaryNeedCheck(SegmentUnit segmentUnit) {
    try {
      SegmentUnitMetadata metadata = segmentUnit.getSegmentUnitMetadata();
      SegmentUnitStatus currentStatus = metadata.getStatus();
      if (currentStatus == SegmentUnitStatus.ModeratorSelected
          || currentStatus == SegmentUnitStatus.SecondaryEnrolled
          || currentStatus == SegmentUnitStatus.Secondary) {
        return true;
      }
      return false;
    } catch (Exception e) {
      logger.warn("secondary expiration check error", e);
      return false;
    }

  }

}
