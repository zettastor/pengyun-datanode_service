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

package py.datanode.service;

import java.nio.ByteBuffer;
import java.util.Objects;
import org.apache.commons.lang.Validate;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.PbRequestResponseHelper;
import py.RequestResponseHelper;
import py.app.context.AppContext;
import py.archive.segment.CloneStatus;
import py.archive.segment.CloneType;
import py.archive.segment.MigrationStatus;
import py.archive.segment.SegId;
import py.archive.segment.SegmentUnitMetadata;
import py.archive.segment.SegmentUnitStatus;
import py.datanode.exception.LazyCloneNotInitializedException;
import py.datanode.segment.SegmentUnit;
import py.instance.InstanceId;
import py.membership.SegmentMembership;
import py.proto.Broadcastlog.PbAsyncSyncLogBatchUnit;
import py.proto.Broadcastlog.PbSegmentUnitStatus;
import py.thrift.datanode.service.ArbiterPokePrimaryRequest;
import py.thrift.datanode.service.BroadcastRequest;
import py.thrift.datanode.service.ImReadyToBePrimaryRequest;
import py.thrift.datanode.service.ImReadyToBeSecondaryRequest;
import py.thrift.datanode.service.InitiateCopyPageRequestThrift;
import py.thrift.datanode.service.InputHasNoDataExceptionThrift;
import py.thrift.datanode.service.InvalidSegmentUnitStatusExceptionThrift;
import py.thrift.datanode.service.JoiningGroupRequest;
import py.thrift.datanode.service.KickOffPotentialPrimaryRequest;
import py.thrift.datanode.service.MakePrimaryDecisionRequest;
import py.thrift.datanode.service.OutOfRangeExceptionThrift;
import py.thrift.datanode.service.PrimaryNeedRollBackFirstExceptionThrift;
import py.thrift.datanode.service.SecondaryCopyPagesRequest;
import py.thrift.datanode.service.SegmentNotFoundExceptionThrift;
import py.thrift.datanode.service.StillAtPrePrimaryExceptionThrift;
import py.thrift.datanode.service.SyncLogsRequest;
import py.thrift.datanode.service.SyncShadowPagesRequest;
import py.thrift.datanode.service.WrongPrePrimarySessionExceptionThrift;
import py.thrift.share.InternalErrorThrift;
import py.thrift.share.InvalidMembershipExceptionThrift;
import py.thrift.share.NotPrimaryExceptionThrift;
import py.thrift.share.NotSecondaryExceptionThrift;
import py.thrift.share.SegmentMembershipThrift;
import py.thrift.share.SegmentUnitBeingDeletedExceptionThrift;
import py.thrift.share.SegmentUnitStatusThrift;
import py.thrift.share.StaleMembershipExceptionThrift;
import py.thrift.share.YouAreNotInMembershipExceptionThrift;
import py.thrift.share.YouAreNotInRightPositionExceptionThrift;
import py.thrift.share.YouAreNotReadyExceptionThrift;

public class Validator {
  private static final Logger logger = LoggerFactory.getLogger(Validator.class);

  public static void validateReadInput(long offsetAtSegment, int length, int pageSize,
      long segUnitSize)
      throws SegmentNotFoundExceptionThrift, OutOfRangeExceptionThrift, NotPrimaryExceptionThrift,
      SegmentUnitBeingDeletedExceptionThrift, InternalErrorThrift, TException {
    try {
      validateOffsetAndLength(offsetAtSegment, length, pageSize, segUnitSize);
    } catch (OutOfRangeExceptionThrift e) {
      logger.error(e.getDetail(), e);
      throw e;
    }
  }

  public static void validateWriteInput(int dataLength, long offsetInSegment, int pageSize,
      long segUnitSize)
      throws InputHasNoDataExceptionThrift, OutOfRangeExceptionThrift {
    if (dataLength == 0) {
      logger.error("the request doesn't contain data");
      throw new InputHasNoDataExceptionThrift();
    }

    try {
      validateOffsetAndLength(offsetInSegment, dataLength, pageSize, segUnitSize);
    } catch (OutOfRangeExceptionThrift e) {
      logger.error(e.getDetail(), e);
      throw e;
    }
  }

  public static void validateWriteInput(byte[] data, long offsetInSegment, int pageSize,
      long segUnitSize)
      throws SegmentNotFoundExceptionThrift, InputHasNoDataExceptionThrift,
      SegmentUnitBeingDeletedExceptionThrift, OutOfRangeExceptionThrift,
      InternalErrorThrift, TException,
      NotPrimaryExceptionThrift {
    int length = data.length;
    if (data == null || length == 0) {
      logger.error("the request doesn't contain data");
      throw new InputHasNoDataExceptionThrift();
    }

    try {
      validateOffsetAndLength(offsetInSegment, length, pageSize, segUnitSize);
    } catch (OutOfRangeExceptionThrift e) {
      logger.error(e.getDetail(), e);
      throw e;
    }
  }

  public static void validateJoinGroupInput(JoiningGroupRequest request, SegmentUnit segUnit,
      AppContext context)
      throws SegmentNotFoundExceptionThrift, NotPrimaryExceptionThrift, TException,
      InvalidMembershipExceptionThrift {
    try {
      validateSegmentUnit(segUnit);
    } catch (SegmentNotFoundExceptionThrift e) {
      logger.info(e.getDetail());
      throw e;
    } catch (SegmentUnitBeingDeletedExceptionThrift e) {
      logger.info(e.getDetail());
      throw new NotPrimaryExceptionThrift(RequestResponseHelper
          .buildThriftMembershipFrom(segUnit.getSegId(),
              segUnit.getSegmentUnitMetadata().getMembership()));
    }

    SegmentMembership currentMembership = segUnit.getSegmentUnitMetadata().getMembership();
    SegId segId = segUnit.getSegId();
    validatePrimaryStatusAndMembership(segId, segUnit.getSegmentUnitMetadata().getStatus(),
        currentMembership,
        context, true);
    Validator.requestNotHavingHigherMembership(segId, currentMembership,
        DataNodeRequestResponseHelper.buildSegmentMembershipFrom(request.getMembership())
            .getSecond());
  }

  public static void validatePresecondaryAtProperSession(long myPreprimaryDrivingSessionId,
      long preprimarySidFromPreSecondary) throws WrongPrePrimarySessionExceptionThrift {
    if (preprimarySidFromPreSecondary != SegmentUnit.EMPTY_PREPRIMARY_DRIVING_SESSION_ID
        && preprimarySidFromPreSecondary != myPreprimaryDrivingSessionId) {
      throw new WrongPrePrimarySessionExceptionThrift().setDetail(
          "my preprimary id is " + myPreprimaryDrivingSessionId + "secondary's is "
              + preprimarySidFromPreSecondary);
    }
  }

  public static void validateSyncShadowPagesInput(SyncShadowPagesRequest request,
      SegmentUnit segUnit,
      AppContext context)
      throws SegmentNotFoundExceptionThrift, NotPrimaryExceptionThrift,
      InvalidMembershipExceptionThrift,
      StaleMembershipExceptionThrift, TException {
    validateSecondaryRequestToPrimaryOrPrePrimary(segUnit, request.getMembership(), context,
        request.getMyself(),
        false);
    validatePresecondaryAtProperSession(segUnit.getPreprimaryDrivingSessionId(),
        request.getPreprimarySid());
  }

  public static void validateSyncLogInput(SyncLogsRequest request, SegmentUnit segUnit,
      AppContext context,
      SegmentUnitStatusThrift requestSegmentUnitStatus)
      throws SegmentNotFoundExceptionThrift, NotPrimaryExceptionThrift,
      WrongPrePrimarySessionExceptionThrift,
      StillAtPrePrimaryExceptionThrift, InvalidMembershipExceptionThrift, 
      StaleMembershipExceptionThrift,
      TException {
    boolean onlyValidatePrimary = false;

    if (segUnit == null) {
      throw new SegmentNotFoundExceptionThrift();
    }

    if (requestSegmentUnitStatus == SegmentUnitStatusThrift.Secondary) {
      onlyValidatePrimary = true;
      SegmentMembership currentMembership = segUnit.getSegmentUnitMetadata().getMembership();
      SegmentMembership requestMembership = DataNodeRequestResponseHelper
          .buildSegmentMembershipFrom(request.getMembership()).getSecond();

      if (segUnit.getSegmentUnitMetadata().getStatus() == SegmentUnitStatus.PrePrimary) {
        if (currentMembership.equals(requestMembership)) {
          throw new StillAtPrePrimaryExceptionThrift();
        }
      }

      InstanceId requestSender = new InstanceId(request.getMyself());
      if (!currentMembership.isAliveSecondaries(requestSender) && !currentMembership
          .isSecondaryCandidate(requestSender)) {
        throw new YouAreNotInMembershipExceptionThrift();
      }
    }

    validateSecondaryRequestToPrimaryOrPrePrimary(segUnit, request.getMembership(), context,
        request.myself,
        onlyValidatePrimary);
    validatePresecondaryAtProperSession(segUnit.getPreprimaryDrivingSessionId(),
        request.getPreprimarySid());
  }

  public static void validateSyncLogInput(PbAsyncSyncLogBatchUnit request, SegmentUnit segUnit,
      AppContext context,
      PbSegmentUnitStatus requestSegmentUnitStatus)
      throws SegmentNotFoundExceptionThrift, NotPrimaryExceptionThrift, 
      WrongPrePrimarySessionExceptionThrift,
      StillAtPrePrimaryExceptionThrift, InvalidMembershipExceptionThrift,
      StaleMembershipExceptionThrift,
      YouAreNotInMembershipExceptionThrift {
    boolean onlyValidatePrimary = false;

    if (segUnit == null) {
      throw new SegmentNotFoundExceptionThrift();
    }

    SegmentMembership currentMembership = segUnit.getSegmentUnitMetadata().getMembership();
    SegmentMembership requestMembership = PbRequestResponseHelper
        .buildMembershipFrom(request.getMembership());
    if (requestSegmentUnitStatus == PbSegmentUnitStatus.SECONDARY) {
      onlyValidatePrimary = true;

      if (segUnit.getSegmentUnitMetadata().getStatus() == SegmentUnitStatus.PrePrimary) {
        if (currentMembership.equals(requestMembership)) {
          throw new StillAtPrePrimaryExceptionThrift();
        }
      }

      InstanceId requestSender = new InstanceId(request.getMyself());
      if (!currentMembership.isAliveSecondaries(requestSender) && !currentMembership
          .isSecondaryCandidate(requestSender)) {
        throw new YouAreNotInMembershipExceptionThrift();
      }
    }

    validateSecondaryRequestToPrimaryOrPrePrimary(segUnit, requestMembership, context,
        request.getMyself(),
        onlyValidatePrimary);
    validatePresecondaryAtProperSession(segUnit.getPreprimaryDrivingSessionId(),
        request.getPreprimarySid());
  }

  public static void validateArbiterPokePrimaryInput(ArbiterPokePrimaryRequest request,
      SegmentUnit segUnit,
      AppContext context)
      throws SegmentNotFoundExceptionThrift, NotPrimaryExceptionThrift,
      WrongPrePrimarySessionExceptionThrift,
      InvalidMembershipExceptionThrift, StaleMembershipExceptionThrift, TException {
    SegmentUnitStatusThrift requestSegmentUnitStatus = request.getRequestSegmentUnitStatus();
    boolean onlyValidatePrimary =
        requestSegmentUnitStatus.compareTo(SegmentUnitStatusThrift.Arbiter) == 0 ? true : false;
    if (onlyValidatePrimary) {
      SegmentMembership membership = segUnit.getSegmentUnitMetadata().getMembership();
      if (!membership.isArbiter(new InstanceId(request.getMyself()))) {
        throw new YouAreNotInMembershipExceptionThrift();
      }
    }

    if (requestSegmentUnitStatus.equals(SegmentUnitStatusThrift.PreArbiter)
        && segUnit.getSegmentUnitMetadata().getStatus() == SegmentUnitStatus.Primary) {
      logger.warn("a pre-arbiter is still poking me, but i am already primary {}",
          segUnit.getSegId());
      throw new WrongPrePrimarySessionExceptionThrift();
    }

    validateSecondaryRequestToPrimaryOrPrePrimary(segUnit, request.getMembership(), context,
        request.myself,
        false);
    validatePresecondaryAtProperSession(segUnit.getPreprimaryDrivingSessionId(),
        request.getPreprimarySid());
  }

  public static void validateImReadyToBeSecondaryInput(ImReadyToBeSecondaryRequest request,
      SegmentUnit segUnit,
      AppContext context)
      throws SegmentNotFoundExceptionThrift, NotPrimaryExceptionThrift,
      WrongPrePrimarySessionExceptionThrift,
      InvalidMembershipExceptionThrift, StaleMembershipExceptionThrift, TException {
    boolean onlyValidatePrimary = true;
    validateSecondaryRequestToPrimaryOrPrePrimary(segUnit, request.getMembership(), context,
        request.myself,
        onlyValidatePrimary);
  }

  public static void validateImReadyToBePrimaryInput(ImReadyToBePrimaryRequest request,
      SegmentUnit segUnit,
      AppContext context)
      throws SegmentNotFoundExceptionThrift, NotPrimaryExceptionThrift, 
      YouAreNotInMembershipExceptionThrift,
      InvalidMembershipExceptionThrift, StaleMembershipExceptionThrift,
      YouAreNotInRightPositionExceptionThrift, YouAreNotReadyExceptionThrift {
    boolean onlyValidatePrimary = true;
    validateSecondaryRequestToPrimaryOrPrePrimary(segUnit, request.getMembership(), context,
        request.myself,
        onlyValidatePrimary);
    if (!segUnit.getSegmentUnitMetadata().getMembership()
        .isPrimaryCandidate(new InstanceId(request.myself))) {
      logger.warn("the request primary candidate {} is not in right position of {}", request.myself,
          segUnit.getSegmentUnitMetadata().getMembership());
      throw new YouAreNotInRightPositionExceptionThrift(RequestResponseHelper
          .buildThriftMembershipFrom(segUnit.getSegId(),
              segUnit.getSegmentUnitMetadata().getMembership()));
    }
    if (segUnit.getSegmentLogMetadata().getClId() != request.getMyClId()) {
      logger.warn("the request primary candidate(whoes cl is {}) is not ready, my cl {}",
          request.getMyClId(),
          segUnit.getSegmentLogMetadata().getClId());
      throw new YouAreNotReadyExceptionThrift();
    }
  }

  public static void validateSecondaryCopyPagesInput(SecondaryCopyPagesRequest request,
      SegmentUnit segUnit,
      AppContext context)
      throws SegmentNotFoundExceptionThrift, NotPrimaryExceptionThrift, TException {
    validateSecondaryRequestToPrimaryOrPrePrimary(segUnit, request.getMembership(), context,
        request.myself, false);
    validatePresecondaryAtProperSession(segUnit.getPreprimaryDrivingSessionId(),
        request.getPreprimarySid());
  }

  public static void validateInitiateCopyPageInput(InitiateCopyPageRequestThrift request,
      SegmentUnit segUnit,
      AppContext context)
      throws SegmentNotFoundExceptionThrift, NotPrimaryExceptionThrift, 
      InvalidMembershipExceptionThrift,
      StaleMembershipExceptionThrift, WrongPrePrimarySessionExceptionThrift,
      YouAreNotInMembershipExceptionThrift {
    validateSecondaryRequestToPrimaryOrPrePrimary(segUnit, request.getMembership(), context,
        request.myself, false);
    validatePresecondaryAtProperSession(segUnit.getPreprimaryDrivingSessionId(),
        request.getPreprimarySid());
  }

  public static void validatePageCloneInput(SegmentUnit segUnit)
      throws SegmentNotFoundExceptionThrift, NotPrimaryExceptionThrift {
    try {
      validateSegmentUnit(segUnit);
    } catch (SegmentNotFoundExceptionThrift e) {
      logger.info(e.getDetail());
      throw e;
    } catch (SegmentUnitBeingDeletedExceptionThrift e) {
      logger.info(e.getDetail());
      throw new NotPrimaryExceptionThrift(RequestResponseHelper
          .buildThriftMembershipFrom(segUnit.getSegId(),
              segUnit.getSegmentUnitMetadata().getMembership()));
    }

    SegmentUnitStatus myStatus = segUnit.getSegmentUnitMetadata().getStatus();
    if (myStatus != SegmentUnitStatus.Primary) {
      throw new NotPrimaryExceptionThrift(RequestResponseHelper
          .buildThriftMembershipFrom(segUnit.getSegId(),
              segUnit.getSegmentUnitMetadata().getMembership()));
    }
  }

  public static void validateMigrationPagesInput(SegmentUnit segUnit,
      SegmentMembership requestMembership,
      AppContext context)
      throws SegmentNotFoundExceptionThrift, NotPrimaryExceptionThrift, TException,
      InvalidMembershipExceptionThrift, StaleMembershipExceptionThrift {
    try {
      validateSegmentUnit(segUnit);
    } catch (SegmentNotFoundExceptionThrift e) {
      logger.info(e.getDetail());
      throw e;
    } catch (SegmentUnitBeingDeletedExceptionThrift e) {
      logger.info(e.getDetail());
      throw new NotPrimaryExceptionThrift(RequestResponseHelper
          .buildThriftMembershipFrom(segUnit.getSegId(),
              segUnit.getSegmentUnitMetadata().getMembership()));
    }

    SegmentMembership currentMembership = segUnit.getSegmentUnitMetadata().getMembership();
    SegId segId = segUnit.getSegId();
    requestNotHavingHigherMembershipEpoch(segId, requestMembership, currentMembership);

    requestNotHavingHigherMembershipGeneration(segId, requestMembership, currentMembership);

    validateSecondaryInMembership(segId, context.getInstanceId().getId(), currentMembership);
    SegmentUnitStatus myStatus = segUnit.getSegmentUnitMetadata().getStatus();
    if (myStatus != SegmentUnitStatus.PreSecondary) {
      throw new TException("status: " + myStatus + " not right");
    }
  }

  private static void validateSecondaryRequestToPrimaryOrPrePrimary(SegmentUnit segUnit,
      SegmentMembershipThrift requestMembershipThrift, AppContext context, long requestorId,
      boolean validateOnlyPrimary)
      throws SegmentNotFoundExceptionThrift, NotPrimaryExceptionThrift,
      InvalidMembershipExceptionThrift,
      StaleMembershipExceptionThrift, YouAreNotInMembershipExceptionThrift {
    try {
      validateSegmentUnit(segUnit);
    } catch (SegmentNotFoundExceptionThrift e) {
      logger.info(e.getDetail());
      throw e;
    } catch (SegmentUnitBeingDeletedExceptionThrift e) {
      logger.info(e.getDetail());
      throw new NotPrimaryExceptionThrift(RequestResponseHelper
          .buildThriftMembershipFrom(segUnit.getSegId(),
              segUnit.getSegmentUnitMetadata().getMembership()));
    }

    SegmentUnitStatus myStatus = segUnit.getSegmentUnitMetadata().getStatus();
    SegmentMembership currentMembership = segUnit.getSegmentUnitMetadata().getMembership();

    if (myStatus == SegmentUnitStatus.Primary || (myStatus == SegmentUnitStatus.OFFLINING
        && currentMembership
        .isPrimary(context.getInstanceId()))) {
      validateSecondaryRequest(segUnit, requestMembershipThrift, context, requestorId);
    } else if (myStatus == SegmentUnitStatus.PrePrimary) {
      if (validateOnlyPrimary) {
        logger.warn("i'm not primary but {} for segId={}", myStatus, segUnit.getSegId());
        throw new NotPrimaryExceptionThrift(
            RequestResponseHelper.buildThriftMembershipFrom(segUnit.getSegId(),
                segUnit.getSegmentUnitMetadata().getMembership())).setDetail(
            " my status is " + myStatus + " my membership is " + segUnit.getSegmentUnitMetadata()
                .getMembership());
      }
      SegmentMembership requestMembership = DataNodeRequestResponseHelper
          .buildSegmentMembershipFrom(requestMembershipThrift).getSecond();

      SegId segId = segUnit.getSegId();

      requestNotHavingHigherMembershipEpoch(segId, currentMembership, requestMembership);

      requestNotHavingHigherMembershipGeneration(segId, currentMembership, requestMembership);

      validateSecondaryInMembership(segId, requestorId, currentMembership);
    } else {
      throw new NotPrimaryExceptionThrift(RequestResponseHelper
          .buildThriftMembershipFrom(segUnit.getSegId(),
              segUnit.getSegmentUnitMetadata().getMembership()))
          .setDetail(
              " my status is " + myStatus + " my membership is " + segUnit.getSegmentUnitMetadata()
                  .getMembership());
    }
  }

  private static void validateSecondaryRequestToPrimaryOrPrePrimary(SegmentUnit segUnit,
      SegmentMembership membership, AppContext context, long requestorId,
      boolean validateOnlyPrimary)
      throws SegmentNotFoundExceptionThrift, NotPrimaryExceptionThrift, 
      InvalidMembershipExceptionThrift,
      StaleMembershipExceptionThrift, YouAreNotInMembershipExceptionThrift {
    try {
      validateSegmentUnit(segUnit);
    } catch (SegmentNotFoundExceptionThrift e) {
      logger.info(e.getDetail());
      throw e;
    } catch (SegmentUnitBeingDeletedExceptionThrift e) {
      logger.info(e.getDetail());
      throw new NotPrimaryExceptionThrift(RequestResponseHelper
          .buildThriftMembershipFrom(segUnit.getSegId(),
              segUnit.getSegmentUnitMetadata().getMembership()));
    }

    SegmentUnitStatus myStatus = segUnit.getSegmentUnitMetadata().getStatus();
    SegmentMembership currentMembership = segUnit.getSegmentUnitMetadata().getMembership();

    if (myStatus == SegmentUnitStatus.Primary || (myStatus == SegmentUnitStatus.OFFLINING
        && currentMembership
        .isPrimary(context.getInstanceId()))) {
      validateSecondaryRequest(segUnit, membership, context, requestorId);
    } else if (myStatus == SegmentUnitStatus.PrePrimary) {
      if (validateOnlyPrimary) {
        logger.warn("i'm not primary but {} for segId={}", myStatus, segUnit.getSegId());
        throw new NotPrimaryExceptionThrift(
            RequestResponseHelper.buildThriftMembershipFrom(segUnit.getSegId(),
                segUnit.getSegmentUnitMetadata().getMembership())).setDetail(
            " my status is " + myStatus + " my membership is " + segUnit.getSegmentUnitMetadata()
                .getMembership());
      }

      SegId segId = segUnit.getSegId();

      requestNotHavingHigherMembershipEpoch(segId, currentMembership, membership);

      requestNotHavingHigherMembershipGeneration(segId, currentMembership, membership);

      validateSecondaryInMembership(segId, requestorId, currentMembership);
    } else {
      throw new NotPrimaryExceptionThrift(RequestResponseHelper
          .buildThriftMembershipFrom(segUnit.getSegId(),
              segUnit.getSegmentUnitMetadata().getMembership()))
          .setDetail(
              " my status is " + myStatus + " my membership is " + segUnit.getSegmentUnitMetadata()
                  .getMembership());
    }
  }

  private static void validateSecondaryRequest(SegmentUnit segUnit,
      SegmentMembershipThrift requestMembershipThrift,
      AppContext context, long requestorId)
      throws SegmentNotFoundExceptionThrift, NotPrimaryExceptionThrift,
      InvalidMembershipExceptionThrift,
      StaleMembershipExceptionThrift, YouAreNotInMembershipExceptionThrift {
    validateSegmentUnitExistsAndImPrimary(segUnit, context);

    SegmentMembership currentMembership = segUnit.getSegmentUnitMetadata().getMembership();
    SegmentMembership requestMembership = DataNodeRequestResponseHelper
        .buildSegmentMembershipFrom(requestMembershipThrift).getSecond();

    SegId segId = segUnit.getSegId();
    requestNotHavingStaleMembershipEpoch(segId, currentMembership, requestMembership);

    requestNotHavingHigherMembershipEpoch(segId, currentMembership, requestMembership);

    requestNotHavingHigherMembershipGeneration(segId, currentMembership, requestMembership);

    validateSecondaryInMembership(segId, requestorId, currentMembership);
  }

  private static void validateSecondaryRequest(SegmentUnit segUnit,
      SegmentMembership segmentMembership,
      AppContext context, long requestorId)
      throws SegmentNotFoundExceptionThrift, NotPrimaryExceptionThrift,
      InvalidMembershipExceptionThrift,
      StaleMembershipExceptionThrift, YouAreNotInMembershipExceptionThrift {
    validateSegmentUnitExistsAndImPrimary(segUnit, context);

    SegmentMembership currentMembership = segUnit.getSegmentUnitMetadata().getMembership();

    SegId segId = segUnit.getSegId();
    requestNotHavingStaleMembershipEpoch(segId, currentMembership, segmentMembership);

    requestNotHavingHigherMembershipEpoch(segId, currentMembership, segmentMembership);

    requestNotHavingHigherMembershipGeneration(segId, currentMembership, segmentMembership);

    validateSecondaryInMembership(segId, requestorId, currentMembership);
  }

  private static void validateSecondaryInMembership(SegId segId, long secondaryId,
      SegmentMembership currentMembership) throws YouAreNotInMembershipExceptionThrift {
    if (!currentMembership.contain(secondaryId)) {
      throw new YouAreNotInMembershipExceptionThrift()
          .setMembership(RequestResponseHelper.buildThriftMembershipFrom(segId, currentMembership));
    }
  }

  public static void validateSegmentUnitExistsAndImPrimary(SegmentUnit segUnit, AppContext context)
      throws SegmentNotFoundExceptionThrift, NotPrimaryExceptionThrift {
    try {
      validateSegmentUnit(segUnit);
    } catch (SegmentNotFoundExceptionThrift e) {
      logger.info(e.getDetail());
      throw e;
    } catch (SegmentUnitBeingDeletedExceptionThrift e) {
      logger.info(e.getDetail());
      throw new NotPrimaryExceptionThrift(RequestResponseHelper
          .buildThriftMembershipFrom(segUnit.getSegId(),
              segUnit.getSegmentUnitMetadata().getMembership()));
    }

    if (segUnit.isAllowReadDataEvenNotPrimary()) {
      return;
    }

    SegmentMembership currentMembership = segUnit.getSegmentUnitMetadata().getMembership();
    SegmentUnitStatus status = segUnit.getSegmentUnitMetadata().getStatus();
    try {
      validatePrimaryStatusAndMembership(segUnit.getSegId(), status, currentMembership, context);
    } catch (NotPrimaryExceptionThrift e) {
      logger.info(e.getDetail());
      throw e;
    }
  }

  public static void validateCanWrite(SegmentUnit segUnit) throws NotSecondaryExceptionThrift {
    if (segUnit.isSecondaryZombie()) {
      return;
    }
    SegmentUnitStatus status = segUnit.getSegmentUnitMetadata().getStatus();
    if (status == SegmentUnitStatus.Secondary) {
      return;
    } else if (status == SegmentUnitStatus.PreSecondary) {
      if (!segUnit.hasParticipatedVotingProcess()) {
        if (segUnit.getSecondaryCopyPageManager() == null) {
          logger.debug("it is initializing for segId={}", segUnit.getSegId());
        }
      }
      return;
    }

    SegmentMembership currentMembership = segUnit.getSegmentUnitMetadata().getMembership();
    SegId segId = segUnit.getSegId();

    throw new NotSecondaryExceptionThrift()
        .setMembership(
            DataNodeRequestResponseHelper.buildThriftMembershipFrom(segId, currentMembership))
        .setDetail("the current status is " + status);
  }

  public static void validateGiveYouLogId(BroadcastRequest giveYouLogIdRequest, SegmentUnit segUnit,
      AppContext context)
      throws SegmentUnitBeingDeletedExceptionThrift, SegmentNotFoundExceptionThrift,
      NotSecondaryExceptionThrift, StaleMembershipExceptionThrift,
      InvalidMembershipExceptionThrift {
    try {
      validateSegmentUnit(segUnit);
    } catch (SegmentNotFoundExceptionThrift e) {
      logger.info(e.getDetail());
      throw e;
    } catch (SegmentUnitBeingDeletedExceptionThrift e) {
      logger.info(e.getDetail());
      throw e;
    }

    SegmentMembership currentMembership = segUnit.getSegmentUnitMetadata().getMembership();
    SegmentMembership requestMembership = DataNodeRequestResponseHelper
        .buildSegmentMembershipFrom(giveYouLogIdRequest.getMembership()).getSecond();
    SegId segId = segUnit.getSegId();

    requestNotHavingStaleMembership(segId, currentMembership, requestMembership);
    requestNotHavingStaleMembershipGeneration(segId, currentMembership, requestMembership);

    validateCanWrite(segUnit);

    if (!amSecondary(currentMembership, context) && !amJoiningSecondary(currentMembership,
        context)) {
      throw new NotSecondaryExceptionThrift().setDetail(
          "I(" + context.getInstanceId()
              + ") am not secondary nor joining secondary in membership");
    }
    long primaryOrTempPrimary = giveYouLogIdRequest.getMyself();
    long primaryKickedOff = segUnit.getPotentialPrimaryId();
    if (primaryKickedOff != SegmentUnit.NO_POTENTIAL_PRIMARY_ID
        && primaryOrTempPrimary != primaryKickedOff) {
      throw new NotSecondaryExceptionThrift()
          .setDetail("another pre primary " + primaryKickedOff + " has been kicked off");
    }

  }

  public static void validateSegmentUnitExistsAndImCanWriteSecondaryOrJoiningSecondary(
      SegmentUnit segUnit,
      AppContext context)
      throws SegmentNotFoundExceptionThrift, NotSecondaryExceptionThrift, TException {
    try {
      validateSegmentUnit(segUnit);
    } catch (SegmentNotFoundExceptionThrift e) {
      logger.info(e.getDetail());
      throw e;
    } catch (SegmentUnitBeingDeletedExceptionThrift e) {
      logger.info(e.getDetail());
      throw new NotSecondaryExceptionThrift(RequestResponseHelper
          .buildThriftMembershipFrom(segUnit.getSegId(),
              segUnit.getSegmentUnitMetadata().getMembership()));
    }

    SegmentMembership currentMembership = segUnit.getSegmentUnitMetadata().getMembership();
    SegId segId = segUnit.getSegId();

    if (currentMembership == null) {
      throw new TException("can't get membersip for segment " + segId);
    }

    if (!amSecondary(currentMembership, context) && !amJoiningSecondary(currentMembership,
        context)) {
      NotSecondaryExceptionThrift e = new NotSecondaryExceptionThrift(
          DataNodeRequestResponseHelper.buildThriftMembershipFrom(segId, currentMembership));
      String errMsg =
          "The server " + context.getInstanceId() + " is not the secondary of the segment:"
              + currentMembership.toString();
      e.setDetail(errMsg);
      throw e;
    }

    validateCanWrite(segUnit);
  }

  public static void validateSegmentUnitExistsAndImSecondary(SegmentUnit segUnit,
      AppContext context)
      throws SegmentNotFoundExceptionThrift, NotSecondaryExceptionThrift, TException {
    try {
      validateSegmentUnit(segUnit);
    } catch (SegmentNotFoundExceptionThrift e) {
      logger.info(e.getDetail());
      throw e;
    } catch (SegmentUnitBeingDeletedExceptionThrift e) {
      logger.info(e.getDetail());
      throw new NotSecondaryExceptionThrift(RequestResponseHelper
          .buildThriftMembershipFrom(segUnit.getSegId(),
              segUnit.getSegmentUnitMetadata().getMembership()));
    }

    SegmentMembership currentMembership = segUnit.getSegmentUnitMetadata().getMembership();
    SegmentUnitStatus status = segUnit.getSegmentUnitMetadata().getStatus();
    try {
      validateSecondaryStatusAndMembership(segUnit.getSegId(), status, currentMembership, context);
    } catch (NotSecondaryExceptionThrift e) {
      logger.info(e.getDetail());
      throw e;
    } catch (TException e) {
      logger.info("caught an exception", e);
      throw e;
    }
  }

  public static void validateBroadcastRequest(BroadcastRequest request, SegmentUnit segUnit,
      AppContext context)
      throws SegmentNotFoundExceptionThrift, SegmentUnitBeingDeletedExceptionThrift {
    try {
      validateSegmentUnit(segUnit);
    } catch (SegmentNotFoundExceptionThrift e) {
      e.setDetail("Can't find the specified segment unit:" + request.getVolumeId() + ":" + request
          .getSegIndex());
      logger.info(e.getDetail());
      throw e;
    } catch (SegmentUnitBeingDeletedExceptionThrift e) {
      e.setDetail(
          "The specified segment unit:" + request.getVolumeId() + " has been marked as Deleting "
              + ":"
              + request.getSegIndex());
      logger.info(e.getDetail());
      throw e;
    }
  }

  public static void validateMakePrimaryDecisionRequest(MakePrimaryDecisionRequest request,
      SegmentUnit segmentUnit,
      AppContext context)
      throws SegmentNotFoundExceptionThrift, InvalidSegmentUnitStatusExceptionThrift,
      InvalidMembershipExceptionThrift, NotPrimaryExceptionThrift {
    try {
      validateSegmentUnit(segmentUnit);
    } catch (SegmentNotFoundExceptionThrift e) {
      logger.info(e.getDetail());
      throw e;
    } catch (SegmentUnitBeingDeletedExceptionThrift e) {
      InvalidSegmentUnitStatusExceptionThrift newEx = new InvalidSegmentUnitStatusExceptionThrift();
      newEx.setDetail(
          "The specified segment unit:" + request.getVolumeId() + " has been marked as Deleting "
              + ":"
              + request.getSegIndex());
      logger.info(e.getDetail());
      throw newEx;
    }

    try {
      validateMembershipOfKickOffPrimaryOrMakePrimaryDecision(segmentUnit,
          request.getLatestMembership(),
          context.getInstanceId());
    } catch (InvalidMembershipExceptionThrift e) {
      logger.info(e.getDetail());
      throw e;
    }

    long myId = context.getInstanceId().getId();
    if (request.getInstanceIdOfPrimaryDecider() != myId) {
      logger.error("Someone send me an make primary decision request, "
              + "but my id {} is no equal to the decider's id {} specified in the request", myId,
          request.getInstanceIdOfPrimaryDecider());
      throw new NotPrimaryExceptionThrift();
    }
  }

  public static void validateKickoffPrimaryRequest(KickOffPotentialPrimaryRequest request,
      SegmentUnit segUnit,
      AppContext context)
      throws SegmentNotFoundExceptionThrift, InvalidMembershipExceptionThrift, 
      NotPrimaryExceptionThrift,
      InvalidSegmentUnitStatusExceptionThrift {
    try {
      validateSegmentUnit(segUnit);
    } catch (SegmentNotFoundExceptionThrift e) {
      logger.info(e.getDetail());
      throw e;
    } catch (SegmentUnitBeingDeletedExceptionThrift e) {
      InvalidSegmentUnitStatusExceptionThrift newEx = new InvalidSegmentUnitStatusExceptionThrift();
      newEx.setDetail(
          "The specified segment unit:" + request.getVolumeId() + " has been marked as Deleting "
              + ":"
              + request.getSegIndex());
      logger.info(e.getDetail());
      throw newEx;
    }

    if (segUnit.getSegmentUnitMetadata().getMigrationStatus() != MigrationStatus.NONE) {
      logger.error("migrating segment unit should not be kicked off as pre-primary {}", segUnit);
      throw new NotPrimaryExceptionThrift();
    }

    try {
      validateMembershipOfKickOffPrimaryOrMakePrimaryDecision(segUnit,
          request.getLatestMembership(),
          context.getInstanceId());
    } catch (InvalidMembershipExceptionThrift e) {
      logger.info(e.getDetail());
      throw e;
    }

    long myId = context.getInstanceId().getId();
    if (request.getPotentialPrimaryId() != myId) {
      logger.error("Someone send me an kickoffPrimary request, "
              + "but my id {} is no equal to the primary id {} specified in the request", myId,
          request.getPotentialPrimaryId());
      throw new NotPrimaryExceptionThrift();
    }
  }

  private static void validateOffsetAndLength(long offsetInSegment, int length, int pageSize,
      long segmentUnitSize)
      throws OutOfRangeExceptionThrift {
    if (length <= 0) {
      OutOfRangeExceptionThrift e = new OutOfRangeExceptionThrift();
      e.setDetail("length is negative:" + length);
      throw e;
    }

    if (offsetInSegment < 0 || offsetInSegment + length > segmentUnitSize) {
      OutOfRangeExceptionThrift e = new OutOfRangeExceptionThrift();
      e.setDetail("offset is out of range. Offset:" + offsetInSegment + " length: " + length
          + " the segment unit size is " + segmentUnitSize);
      throw e;
    }

    if (length > pageSize) {
      OutOfRangeExceptionThrift e = new OutOfRangeExceptionThrift();
      e.setDetail("the length " + length + " is larger than page size:" + pageSize);
      throw e;
    } else if ((offsetInSegment + length - 1) / pageSize != (offsetInSegment / pageSize)) {
      OutOfRangeExceptionThrift e = new OutOfRangeExceptionThrift();
      e.setDetail("the request is across multiple pages");
      throw e;
    }

  }

  private static void validateSegmentUnit(SegmentUnit segUnit)
      throws SegmentNotFoundExceptionThrift, SegmentUnitBeingDeletedExceptionThrift {
    if (segUnit == null || segUnit.getSegmentUnitMetadata() == null || segUnit
        .getSegmentUnitMetadata()
        .getStatus().isFinalStatus()) {
      SegmentNotFoundExceptionThrift e = new SegmentNotFoundExceptionThrift();
      throw e;
    }

    if (segUnit.getSegmentUnitMetadata().isSegmentUnitMarkedAsDeleting()) {
      throw new SegmentUnitBeingDeletedExceptionThrift();
    }
  }

  private static void validateMembershipOfKickOffPrimaryOrMakePrimaryDecision(SegmentUnit segUnit,
      SegmentMembershipThrift requestMembership, InstanceId myInstanceId)
      throws InvalidMembershipExceptionThrift {
    if (requestMembership != null) {
      SegmentMembership receivedSegmentMembership = DataNodeRequestResponseHelper
          .buildSegmentMembershipFrom(requestMembership).getSecond();
      SegmentMembership currentMembership = segUnit.getSegmentUnitMetadata().getMembership();
      if (receivedSegmentMembership.compareVersion(currentMembership) < 0) {
        String errString =
            "the request has a lower segment membership than mine. " 
                + "throw an exception. RequestMembership:"
                + requestMembership + " current membership: " + currentMembership;
        logger.warn(errString);
        throw new InvalidMembershipExceptionThrift(
            DataNodeRequestResponseHelper
                .buildThriftMembershipFrom(segUnit.getSegId(), currentMembership))
            .setDetail(errString);
      } else {
        if (!receivedSegmentMembership.contain(myInstanceId)) {
          String errString =
              "the request's membership doesn't contain me. throw an exception. RequestMembership:"
                  + requestMembership + " my instance id " + myInstanceId;
          logger.warn(errString);
          throw new InvalidMembershipExceptionThrift(DataNodeRequestResponseHelper
              .buildThriftMembershipFrom(segUnit.getSegId(), currentMembership))
              .setDetail(errString);
        }
      }
    }
  }

  public static void validatePrimaryStatusAndMembership(SegId segId, SegmentUnitStatus status,
      SegmentMembership currentMembership, AppContext context) throws NotPrimaryExceptionThrift {
    validatePrimaryStatusAndMembership(segId, status, currentMembership, context, false);
  }

  public static void validatePrimaryStatusAndMembership(SegId segId, SegmentUnitStatus status,
      SegmentMembership currentMembership, AppContext context, boolean primaryCloneAlsoOk)
      throws NotPrimaryExceptionThrift {
    if (currentMembership == null) {
      throw new IllegalArgumentException("can't get membersip for segment " + segId);
    }

    if (!amPrimary(currentMembership, context)) {
      NotPrimaryExceptionThrift e = new NotPrimaryExceptionThrift(
          DataNodeRequestResponseHelper.buildThriftMembershipFrom(segId, currentMembership));
      String errMsg =
          "The server " + context.getInstanceId() + " is not the primary of the segment:"
              + currentMembership
              .toString();
      e.setDetail(errMsg);
      throw e;
    }

    if (status == SegmentUnitStatus.Primary) {
      return;
    }

    throw new NotPrimaryExceptionThrift()
        .setMembership(
            DataNodeRequestResponseHelper.buildThriftMembershipFrom(segId, currentMembership))
        .setDetail("the current status is " + status);
  }

  public static void validateSecondaryStatusAndMembership(SegId segId, SegmentUnitStatus status,
      SegmentMembership currentMembership, AppContext context)
      throws TException, NotSecondaryExceptionThrift {
    if (currentMembership == null) {
      throw new TException("can't get membersip for segment " + segId);
    }

    if (!amSecondary(currentMembership, context)) {
      NotSecondaryExceptionThrift e = new NotSecondaryExceptionThrift(
          DataNodeRequestResponseHelper.buildThriftMembershipFrom(segId, currentMembership));
      String errMsg =
          "The server " + context.getInstanceId() + " is not the primary of the segment:"
              + currentMembership
              .toString();
      e.setDetail(errMsg);
      throw e;
    }

    if (status != SegmentUnitStatus.Secondary) {
      throw new NotSecondaryExceptionThrift()
          .setMembership(
              DataNodeRequestResponseHelper.buildThriftMembershipFrom(segId, currentMembership))
          .setDetail("the current status is " + status);
    }
  }

  public static void requestNotHavingHigherMembership(SegId segId,
      SegmentMembership currentMembership,
      SegmentMembership requestMembership) throws InvalidMembershipExceptionThrift {
    if (requestHavingHigherMembership(segId, currentMembership, requestMembership)) {
      throw new InvalidMembershipExceptionThrift(
          DataNodeRequestResponseHelper.buildThriftMembershipFrom(segId, currentMembership));
    }
  }

  public static void requestNotHavingStaleMembership(SegId segId,
      SegmentMembership currentMembership,
      SegmentMembership requestMembership) throws StaleMembershipExceptionThrift {
    if (requestHavingStaleMembership(segId, currentMembership, requestMembership)) {
      throw new StaleMembershipExceptionThrift(
          DataNodeRequestResponseHelper.buildThriftMembershipFrom(segId, currentMembership));
    }
  }

  public static void requestNotHavingDifferentTempPrimary(SegId segId,
      SegmentMembership currentMembership, SegmentMembership requestMembership)
      throws StaleMembershipExceptionThrift {
    if (requestHavingDifferentTempPrimary(segId, currentMembership, requestMembership)) {
      throw new StaleMembershipExceptionThrift(
          DataNodeRequestResponseHelper.buildThriftMembershipFrom(segId, currentMembership));
    }
  }

  public static void requestNotHavingStaleEpoch(SegId segId, SegmentMembership currentMembership,
      SegmentMembership requestMembership) throws StaleMembershipExceptionThrift {
    if (requestHavingStaleEpoch(segId, currentMembership, requestMembership)) {
      throw new StaleMembershipExceptionThrift(
          DataNodeRequestResponseHelper.buildThriftMembershipFrom(segId, currentMembership));
    }
  }

  public static boolean requestHavingLowerEpoch(SegId segId, SegmentMembership currentMembership,
      SegmentMembership requestMembership) {
    if (requestMembership == null) {
      logger.warn("request do not have any membership, segId:{}", segId);
      return true;
    }
    if (requestMembership.compareEpoch(currentMembership) < 0) {
      return true;
    }
    return false;
  }

  public static boolean requestHavingLowerGeneration(SegId segId,
      SegmentMembership currentMembership,
      SegmentMembership requestMembership) {
    if (requestMembership == null) {
      logger.warn("request do not have any membership, segId:{}", segId);
      return true;
    }
    if (requestMembership.compareGeneration(currentMembership) < 0) {
      return true;
    }
    return false;
  }

  public static void requestNotHavingInvalidMembership(SegId segId,
      SegmentMembership currentMembership,
      SegmentMembership requestMembership) throws InvalidMembershipExceptionThrift {
    int compareResult = requestMembership.compareVersion(currentMembership);
    if (compareResult == 0 && !requestMembership.equals(currentMembership)) {
      throw new InvalidMembershipExceptionThrift(
          DataNodeRequestResponseHelper.buildThriftMembershipFrom(segId, currentMembership));
    }
  }

  private static boolean amPrimary(SegmentMembership membership, AppContext context) {
    return membership.getPrimary().equals(context.getInstanceId());
  }

  private static boolean amSecondary(SegmentMembership membership, AppContext context) {
    return membership.getSecondaries().contains(context.getInstanceId());
  }

  private static boolean amJoiningSecondary(SegmentMembership membership, AppContext context) {
    return membership.getJoiningSecondaries().contains(context.getInstanceId());

  }

  public static void requestNotHavingStaleMembershipGeneration(SegId segId,
      SegmentMembership currentMembership,
      SegmentMembership requestMembership) throws InvalidMembershipExceptionThrift {
    if (requestMembership.hasSameEpochLowerGeneration(currentMembership)) {
      logger.warn(
          "the broadcast request has a lower segment generation than mine." 
              + " throw an exception. RequestMembership: {} "
              + " current membership: {} ", requestMembership, currentMembership);
      throw new InvalidMembershipExceptionThrift().setLatestMembership(
          DataNodeRequestResponseHelper.buildThriftMembershipFrom(segId, currentMembership));
    }
  }

  public static void requestNotHavingStaleMembershipEpoch(SegId segId,
      SegmentMembership currentMembership,
      SegmentMembership requestMembership) throws StaleMembershipExceptionThrift {
    if (requestMembership.compareEpoch(currentMembership) < 0) {
      logger.warn(
          "the broadcast request has a lower membership epoch than mine." 
              + " throw an exception. RequestMembership: {} "
              + " current membership: {} ", requestMembership, currentMembership);
      throw new StaleMembershipExceptionThrift(
          DataNodeRequestResponseHelper.buildThriftMembershipFrom(segId, currentMembership));
    }
  }

  public static void requestNotHavingHigherMembershipEpoch(SegId segId,
      SegmentMembership currentMembership,
      SegmentMembership requestMembership) throws InvalidMembershipExceptionThrift {
    if (requestMembership.compareEpoch(currentMembership) > 0) {
      logger.warn(
          "the broadcast request has a higher segment membership epoch than mine. " 
              + "throw an exception. RequestMembership: {} "
              + " current membership: {} ", requestMembership, currentMembership);
      throw new InvalidMembershipExceptionThrift(
          DataNodeRequestResponseHelper.buildThriftMembershipFrom(segId, currentMembership));
    }
  }

  public static void requestNotHavingHigherMembershipGeneration(SegId segId,
      SegmentMembership currentMembership,
      SegmentMembership requestMembership) throws InvalidMembershipExceptionThrift {
    if (requestMembership.hasSameEpochHigherGeneration(currentMembership)) {
      logger.warn(
          "the broadcast request has a higher segment membership generation than mine. " 
              + "throw an exception. RequestMembership: {} "
              + " current membership: {} ", requestMembership, currentMembership);
      throw new InvalidMembershipExceptionThrift(
          DataNodeRequestResponseHelper.buildThriftMembershipFrom(segId, currentMembership));
    }
  }

  private static boolean requestHavingHigherMembership(SegId segId,
      SegmentMembership currentMembership,
      SegmentMembership requestMembership) {
    if (requestMembership != null) {
      if (requestMembership.compareVersion(currentMembership) > 0) {
        logger.warn(
            "the request has a higher segment membership than mine. " 
                + "throw an exception. RequestMembership:"
                + " current membership: {} ", requestMembership, currentMembership);
        return true;
      }
    }
    return false;
  }

  private static boolean requestHavingStaleMembership(SegId segId,
      SegmentMembership currentMembership,
      SegmentMembership requestMembership) {
    if (requestMembership == null) {
      logger.warn("request membership is null");
      return true;
    }

    if (requestMembership.compareVersion(currentMembership) < 0) {
      logger.warn(
          "the request has a lower segment membership than mine." 
              + " throw an exception. RequestMembership:"
              + requestMembership + " current membership: " + currentMembership + "for segId:"
              + segId);
      return true;
    }

    return false;
  }

  private static boolean requestHavingDifferentTempPrimary(SegId segId,
      SegmentMembership currentMembership,
      SegmentMembership requestMembership) {
    if (requestMembership == null) {
      logger.warn("request membership is null");
      return true;
    }

    if (!Objects.equals(requestMembership.getTempPrimary(), currentMembership.getTempPrimary())) {
      logger.warn("{} the request has a lower epoch than mine. throw an exception. "
              + "RequestMembership:{} current membership: {}", segId,
          requestMembership, currentMembership);
      return true;
    }

    return false;
  }

  private static boolean requestHavingStaleEpoch(SegId segId, SegmentMembership currentMembership,
      SegmentMembership requestMembership) {
    if (requestMembership == null) {
      logger.warn("request membership is null");
      return true;
    }

    if (requestMembership.compareEpoch(currentMembership) < 0) {
      logger.warn("the request has a lower epoch than mine. throw an exception. RequestMembership:"
          + requestMembership + " current membership: " + currentMembership + "for segId:" + segId);
      return true;
    }

    return false;
  }

  public static void imPrimary(SegmentMembership membership, long memberId)
      throws NotPrimaryExceptionThrift {
    if (!isPrimary(membership, memberId)) {
      throw new NotPrimaryExceptionThrift();
    }
  }

  private static boolean isPrimary(SegmentMembership membership, long memberId) {
    if (membership.getPrimary().getId() != memberId) {
      logger.warn("the sender of the request is not the primary of the group: {} ", membership);
      return false;
    } else {
      return true;
    }
  }

}
