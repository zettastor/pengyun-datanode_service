/*
 * Copyright (c) 2022-2022. PengYunNetWork
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

package py.datanode.service.io;

import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.PbRequestResponseHelper;
import py.app.context.AppContext;
import py.archive.segment.MigrationStatus;
import py.archive.segment.SegId;
import py.archive.segment.SegmentUnitMetadata;
import py.archive.segment.SegmentUnitStatus;
import py.common.struct.Pair;
import py.datanode.segment.SegmentUnit;
import py.instance.InstanceId;
import py.membership.SegmentMembership;
import py.netty.datanode.NettyExceptionHelper;
import py.netty.exception.AbstractNettyException;
import py.netty.exception.HasNewPrimaryException;
import py.netty.exception.IncompleteGroupException;
import py.netty.exception.SegmentNotFoundException;
import py.proto.Broadcastlog;
import py.proto.Broadcastlog.PbIoUnitResult;
import py.proto.Broadcastlog.ReadCause;

public class IoValidator {
  private static final Logger logger = LoggerFactory.getLogger(IoValidator.class);

  public static void validateReadRequest(SegId segId, SegmentUnit segUnit, AppContext context,
      SegmentMembership other, boolean isPrimary, Broadcastlog.ReadCause readCause)
      throws AbstractNettyException {
    validateSegmentUnit(segId, segUnit, context);

    if (segUnit.isAllowReadDataEvenNotPrimary()) {
      return;
    }

    SegmentUnitMetadata metadata = segUnit.getSegmentUnitMetadata();
    MigrationStatus status = metadata.getMigrationStatus();
    if (!isPrimary && readCause == Broadcastlog.ReadCause.FETCH && status.isMigratedStatus()) {
      throw NettyExceptionHelper.buildServerProcessException(
          "migration status=" + status + ", status=" + metadata.getStatus() + ", metadata="
              + metadata
              .getSegId());
    }

    SegmentMembership membership = metadata.getMembership();
    validateMembership(segId, other, membership, context, isPrimary);

    if (isPrimary && readCause == ReadCause.FETCH && membership.getTempPrimary() != null) {
      throw NettyExceptionHelper.buildServerProcessException(
          "trying to read from primary, while temp primary already exists");
    }
  }

  public static void validateMembership(SegId segId, SegmentMembership remote,
      SegmentMembership local,
      AppContext context, boolean isPrimary) throws AbstractNettyException {
    int epochDiff = remote.compareEpoch(local);
    if (epochDiff == 0) {
      int generationDiff = remote.compareGeneration(local);
      if (generationDiff < 0) {
        throw NettyExceptionHelper.buildMembershipVersionLowerException(segId, local);
      } else if (generationDiff == 0) {
        if (isPrimary) {
          Validate.isTrue(local.getPrimary().equals(context.getInstanceId()));
        } else {
          Validate.isTrue(local.getAllSecondaries().contains(context.getInstanceId()) || local
              .isSecondaryCandidate(context.getInstanceId()));
        }
      } else {
        if (!local.getAllSecondaries().contains(context.getInstanceId()) && !local
            .isSecondaryCandidate(context.getInstanceId())) {
          logger.error("coordinator={}, local={}, my instance={}, segId={}", remote, local,
              context.getInstanceId(), segId);
        }
      }
    } else if (epochDiff < 0) {
      throw NettyExceptionHelper.buildMembershipVersionLowerException(segId, local);
    } else {
      throw NettyExceptionHelper.buildMembershipVersionHigerException(segId, local);
    }
  }

  public static void validateSegmentUnit(SegId segId, SegmentUnit segUnit, AppContext context)
      throws AbstractNettyException {
    if (segUnit == null || segUnit.getSegmentUnitMetadata() == null) {
      throw NettyExceptionHelper.buildSegmentNotFoundException(segId, context);
    }

    if (segUnit.getSegmentUnitMetadata().isSegmentUnitDeleted() || segUnit.getSegmentUnitMetadata()
        .isSegmentUnitMarkedAsDeleting() || segUnit.getSegmentUnitMetadata()
        .isSegmentUnitBroken()) {
      throw NettyExceptionHelper.buildSegmentDeleteException(segId, context);
    }
  }

  public static PbIoUnitResult validateReadInput(long offsetAtSegment, int length, int pageSize,
      long segUnitSize) {
    return validateOffsetAndLength(offsetAtSegment, length, pageSize, segUnitSize);
  }

  public static PbIoUnitResult validateWriteInput(int dataLength, long offsetInSegment,
      int pageSize,
      long segUnitSize) {
    return validateOffsetAndLength(offsetInSegment, dataLength, pageSize, segUnitSize);
  }

  private static PbIoUnitResult validateOffsetAndLength(long offsetInSegment, int length,
      int pageSize,
      long segmentUnitSize) {
    if (length == 0) {
      logger.warn("length is zero:{}", length);
      return PbIoUnitResult.INPUT_HAS_NO_DATA;
    }

    if (length < 0) {
      logger.warn("length is negative:{}", length);
      return PbIoUnitResult.OUT_OF_RANGE;
    }

    if (offsetInSegment < 0 || offsetInSegment + length > segmentUnitSize) {
      logger.warn("offset is out of range. Offset: {} length: {} the segment unit size is {}",
          offsetInSegment,
          length, segmentUnitSize);
      return PbIoUnitResult.OUT_OF_RANGE;
    }

    if (length > pageSize) {
      logger.warn("the length: {} is larger than page size: {}", length, pageSize);
      return PbIoUnitResult.OUT_OF_RANGE;
    } else if ((offsetInSegment + length - 1) / pageSize != (offsetInSegment / pageSize)) {
      logger.warn(
          "the request is across multiple pages, offsetInSegment: {}, length: {}, pageSize: {}",
          offsetInSegment, length, pageSize);
      return PbIoUnitResult.OUT_OF_RANGE;
    }

    return PbIoUnitResult.OK;
  }

  public static boolean isPrimary(SegId segId, SegmentMembership membership, AppContext context)
      throws SegmentNotFoundException {
    if (membership.getPrimary().getId() == context.getInstanceId().getId()) {
      return true;
    }

    if (membership.getAllSecondaries().contains(context.getInstanceId()) || membership
        .isSecondaryCandidate(context.getInstanceId())) {
      return false;
    }

    throw NettyExceptionHelper.buildSegmentNotFoundException(segId, context);
  }

  public static SegmentUnitStatus validateSecondaryWriteRequest(SegId segId,
      SegmentMembership requestMembership,
      SegmentUnit segUnit, AppContext context) throws AbstractNettyException {
    validateSegmentUnit(segId, segUnit, context);

    SegmentMembership currentMembership = segUnit.getSegmentUnitMetadata().getMembership();
    validateMembership(segId, requestMembership, currentMembership, context, false);

    SegmentUnitStatus status = segUnit.getSegmentUnitMetadata().getStatus();
    if (status.hasGone()) {
      throw NettyExceptionHelper.buildSegmentDeleteException(segId, context);
    }
    return status;
  }

  public static Pair<SegmentUnitStatus, Boolean> validateZombieWrite(SegId segId,
      SegmentMembership requestMembership,
      SegmentUnit segUnit, AppContext context, boolean isPrimary, boolean syncPersist)
      throws AbstractNettyException {
    validateSegmentUnit(segId, segUnit, context);
    SegmentMembership currentMembership = segUnit.getSegmentUnitMetadata().getMembership();
    SegmentUnitStatus status = segUnit.getSegmentUnitMetadata().getStatus();

    if (requestMembership.compareTo(currentMembership) < 0
        || isPrimary) {
      throw NettyExceptionHelper
          .buildMembershipVersionLowerException(segUnit.getSegId(), currentMembership);
    }

    if (currentMembership.compareTo(requestMembership) < 0) {
      logger.warn("the requester has a higher membership at {}. Ours: {}, Theirs: {}", segId,
          currentMembership,
          requestMembership);
      try {
        if (syncPersist) {
          segUnit.getArchive()
              .updateSegmentUnitMetadata(segId, requestMembership, SegmentUnitStatus.Start);
        } else {
          logger.warn("{} update membership but will be persisted asyncly", segId);
          segUnit.getArchive()
              .asyncUpdateSegmentUnitMetadata(segId, requestMembership, SegmentUnitStatus.Start);
        }
        currentMembership = segUnit.getSegmentUnitMetadata().getMembership();
      } catch (Exception e) {
        logger.error("", e);
      }

    }

    if (!currentMembership.getAllSecondaries().contains(context.getInstanceId())) {
      throw NettyExceptionHelper.buildSegmentNotFoundException(segUnit.getSegId(), context);
    }

    InstanceId tempPrimary = currentMembership.getTempPrimary();
    if (!segUnit.isSecondaryZombie() || tempPrimary == null) {
      logger.warn("isSecondaryZombie {} or no tempPrimary in currentMembership {} seg {}",
          segUnit.isSecondaryZombie(), currentMembership, segId);
      throw NettyExceptionHelper
          .buildNotSecondaryZombieException(segUnit.getSegId(), status, currentMembership);
    }

    long primaryKickedOff = segUnit.getPotentialPrimaryId();
    boolean isTempPrimary = context.getInstanceId().getId() == tempPrimary.getId();
    if (primaryKickedOff != SegmentUnit.NO_POTENTIAL_PRIMARY_ID && primaryKickedOff != tempPrimary
        .getId()) {
      logger
          .warn("primary kicked off={}, tempPrimary={}, {}", primaryKickedOff, tempPrimary, segId);
      throw NettyExceptionHelper
          .buildNotSecondaryException(segUnit.getSegId(), status, currentMembership);
    }

    return new Pair<>(status, isTempPrimary);
  }

  public static SegmentUnitStatus validatePrimaryWriteRequest(SegId segId,
      SegmentMembership requestMembership,
      SegmentUnit segUnit, AppContext context, boolean allowUnstablePrimary)
      throws AbstractNettyException {
    validateSegmentUnit(segId, segUnit, context);

    SegmentMembership currentMembership = segUnit.getSegmentUnitMetadata().getMembership();
    validateMembership(segId, requestMembership, currentMembership, context, true);
    SegmentUnitStatus status = segUnit.getSegmentUnitMetadata().getStatus();

    if (status.hasGone()) {
      throw NettyExceptionHelper.buildSegmentDeleteException(segId, context);
    }

    if (!status.isPrimary() && !allowUnstablePrimary) {
      logger.error("context {} seg {}", context, segUnit);
      throw NettyExceptionHelper.buildNotPrimaryException(segId, status, currentMembership);
    }

    if (currentMembership.getWriteSecondaries().isEmpty()) {
      if (!allowUnstablePrimary && !segUnit.getPclDrivingType(context.getInstanceId())
          .isPrimary()) {
        logger.warn("I can't accept write request right now {}", segUnit);
        throw new IncompleteGroupException();
      } else {
        return status;
      }
    } else {
      return status;
    }
  }

  public static void validateGiveYouLogIdRequest(SegId segId, SegmentUnit segmentUnit,
      AppContext context,
      Broadcastlog.GiveYouLogIdRequest request, boolean syncPersist) throws AbstractNettyException {
    validateSegmentUnit(segId, segmentUnit, context);

    SegmentMembership currentMembership = segmentUnit.getSegmentUnitMetadata().getMembership();
    SegmentMembership requestMembership = PbRequestResponseHelper
        .buildMembershipFrom(request.getPbMembership());

    if (requestMembership.compareTo(currentMembership) > 0) {
      try {
        logger
            .warn("update membership {} to {} seg {}", currentMembership, requestMembership, segId);

        if (syncPersist) {
          segmentUnit.getArchive().updateSegmentUnitMetadata(segId, requestMembership, null);
        } else {
          logger.warn("{} update membership but will be persisted asyncly", segId);
          segmentUnit.getArchive().asyncUpdateSegmentUnitMetadata(segId, requestMembership, null);
        }
        currentMembership = segmentUnit.getSegmentUnitMetadata().getMembership();
      } catch (Exception e) {
        throw NettyExceptionHelper.buildServerProcessException("can't update membership");
      }
    }

    validateMembership(segId, requestMembership, currentMembership, context, false);

    if (!currentMembership.isSecondary(context.getInstanceId()) && !currentMembership
        .isJoiningSecondary(context.getInstanceId()) && !currentMembership
        .isArbiter(context.getInstanceId())
        && !currentMembership.isSecondaryCandidate(context.getInstanceId())) {
      logger.error("{} I({}) am not in the right position of the membership {}", segId,
          context.getInstanceId(),
          currentMembership);
      throw NettyExceptionHelper
          .buildNotSecondaryException(segId, segmentUnit.getSegmentUnitMetadata().getStatus(),
              currentMembership);
    }

    long primaryOrTempPrimary = request.getMyInstanceId();
    long primaryKickedOff = segmentUnit.getPotentialPrimaryId();
    if (primaryKickedOff != SegmentUnit.NO_POTENTIAL_PRIMARY_ID
        && primaryOrTempPrimary != primaryKickedOff) {
      logger.error("another pre primary {} has been kicked off, I can't accept request from {}",
          primaryKickedOff,
          primaryOrTempPrimary, new Exception());
      throw new HasNewPrimaryException();
    }
  }
}
