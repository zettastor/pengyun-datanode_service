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

import org.apache.commons.lang3.Validate;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.segment.SegmentUnitStatus;
import py.client.thrift.GenericThriftClientFactory;
import py.datanode.archive.RawArchiveManager;
import py.datanode.configuration.DataNodeConfiguration;
import py.datanode.segment.SegmentUnit;
import py.datanode.segment.membership.statemachine.StateProcessor;
import py.exception.GenericThriftClientFactoryException;
import py.exception.LeaseExtensionFrozenException;
import py.instance.Instance;
import py.instance.InstanceId;
import py.membership.SegmentMembership;
import py.membership.SegmentMembershipHelper;
import py.thrift.datanode.service.DataNodeService;
import py.thrift.datanode.service.DataNodeService.Iface;
import py.thrift.datanode.service.ImReadyToBeSecondaryRequest;
import py.thrift.datanode.service.ImReadyToBeSecondaryResponse;
import py.thrift.datanode.service.NoOneCanBeReplacedExceptionThrift;
import py.thrift.datanode.service.SegmentNotFoundExceptionThrift;
import py.thrift.share.InvalidMembershipExceptionThrift;
import py.thrift.share.NotPrimaryExceptionThrift;
import py.thrift.share.SegmentMembershipThrift;
import py.thrift.share.ServiceHavingBeenShutdownThrift;
import py.thrift.share.StaleMembershipExceptionThrift;
import py.thrift.share.YouAreNotInMembershipExceptionThrift;

public class ReadyToBecomeSecondary {
  private static final Logger logger = LoggerFactory.getLogger(ReadyToBecomeSecondary.class);

  public static NotifyPrimaryResult notifyPrimary(
      GenericThriftClientFactory<Iface> dataNodeSyncClientFactory,
      RawArchiveManager archiveManager, DataNodeConfiguration cfg, SegmentUnit segUnit,
      SegmentMembership currentMembership, StateProcessor stateProcessor, Instance primaryInstance,
      InstanceId myself, SegmentUnitStatus presecondaryStatus) {
    Validate.isTrue(!segUnit.isArbiter());
    ImReadyToBeSecondaryRequest request = DataNodeRequestResponseHelper
        .buildImReadyToBeSecondaryRequest(segUnit.getSegId(), currentMembership, myself.getId(),
            presecondaryStatus);
    if (segUnit.getSegmentUnitMetadata().isSecondaryCandidate()) {
      request.setSecondaryCandidate(true);
      InstanceId replacee = segUnit.getSegmentUnitMetadata().getReplacee();
      if (replacee != null) {
        request.setReplacee(replacee.getId());
      }
    }

    ImReadyToBeSecondaryResponse response = null;
    SegmentMembershipThrift returnedMembershipThrift = null;
    SegmentUnitStatus newStatus = null;

    DataNodeService.Iface client = null;
    boolean success = false;
    try {
      client = dataNodeSyncClientFactory
          .generateSyncClient(primaryInstance.getEndPoint(), cfg.getDataNodeRequestTimeoutMs());
      response = client.iAmReadyToBeSecondary(request);
      logger.debug("IAmReadyToBeSecondary response {} ", response);
      returnedMembershipThrift = response.getLatestMembership();

      segUnit.extendMyLease();
      success = true;
    } catch (GenericThriftClientFactoryException e) {
      logger.warn("Caught an GenerincThriftClientFactoryException. {} "
              + "Can't get a client to the primary {}" 
              + " request: {}. Change to Start", segUnit.getSegId(),
          primaryInstance, request);
      newStatus = SegmentUnitStatus.Start;
    } catch (ServiceHavingBeenShutdownThrift | SegmentNotFoundExceptionThrift e) {
      logger.warn(
          "{} can't find the segment unit or service " 
              + "has been shutdown. {} Changing my status to start {}",
          primaryInstance, segUnit.getSegId(), request);
      newStatus = SegmentUnitStatus.Start;
    } catch (StaleMembershipExceptionThrift e) {
      logger.warn("{} has lower (epoch) than the primary {} "
              + ". Change status Start to catch up logs {} ",
          myself, primaryInstance, request);
      returnedMembershipThrift = e.getLatestMembership();
      newStatus = SegmentUnitStatus.Start;
    } catch (NotPrimaryExceptionThrift e) {
      logger.warn(
          "primary {} doesn't believe it is the primary any " 
              + "more. Changing {} status from Secondary to Start {}",
          primaryInstance, myself, request, e);
      returnedMembershipThrift = e.getMembership();
      Validate.notNull(returnedMembershipThrift);
      newStatus = SegmentUnitStatus.Start;
    } catch (YouAreNotInMembershipExceptionThrift e) {
      logger.error(
          "I am not at the primary's membership " 
              + "PreArbiter/Arbiter: {} " 
              + " primary: {} "
              + " my membership: {} " 
              + " the primary membership:{} and request {} ", myself,
          primaryInstance,
          currentMembership, e.getMembership(), request);
      newStatus = SegmentUnitStatus.Start;
    } catch (InvalidMembershipExceptionThrift e) {
      logger.error(
          "**************** It is impossible that" 
              + " InvalidMembershipException can be received at a secondary side *******");
      logger.error(
          "I am not at the primary's membership "
              + "PreArbiter/Arbiter: {} "
              + " primary: {} "
              + " my membership: {} " 
              + " the primary membership:{} and request {} ", myself,
          primaryInstance,
          currentMembership, e.getLatestMembership(), request);
      logger.error("Really don't know how to do, change my status to Start");
      newStatus = SegmentUnitStatus.Start;
    } catch (LeaseExtensionFrozenException e) {
      logger.trace(
          "Myself {} can't extend lease but still go ahead to" 
              + " process the lease. the primary is {} and segId is {} ",
          myself, primaryInstance, segUnit.getSegId());
    } catch (NoOneCanBeReplacedExceptionThrift e) {
      logger.warn(
          "I am a secondary candidate, but no one in the membership " 
              + "can be replaced by me, changing myself to Deleting {}, membership: {}",
          segUnit.getSegId(), currentMembership);

      newStatus = SegmentUnitStatus.Deleting;
    } catch (TException e) {
      logger.warn(
          "Caught an mystery exception when syncing log from {} do nothing and retry later　{}",
          primaryInstance, request, e);
      return new NotifyPrimaryResult(false, null, null);
    } finally {
      dataNodeSyncClientFactory.releaseSyncClient(client);
    }

    SegmentMembership returnedMembership = null;
    if (returnedMembershipThrift != null) {
      returnedMembership = DataNodeRequestResponseHelper
          .buildSegmentMembershipFrom(returnedMembershipThrift)
          .getSecond();
      if (returnedMembership.compareVersion(currentMembership) <= 0) {
        returnedMembership = null;
      } else if (SegmentMembershipHelper
          .okToUpdateToHigherMembership(returnedMembership, currentMembership, myself,
              segUnit.getSegmentUnitMetadata().getVolumeType().getNumMembers())) {
        logger.info("the primary has sent a membership with higher generation. ");
      } else {
        logger.warn(
            "My newStatus is supposed to be {} but since it is " 
                + "not ok to update to membership {}  my membership" 
                + " is {}, change my status to Deleting",
            newStatus, returnedMembership, currentMembership);

        returnedMembership = null;
        newStatus = SegmentUnitStatus.Deleting;
      }
    }

    return new NotifyPrimaryResult(success, newStatus, returnedMembership);
  }

  public static class NotifyPrimaryResult {
    private final boolean success;
    private final SegmentMembership newMembership;
    private SegmentUnitStatus newStatus;

    public NotifyPrimaryResult(boolean success, SegmentUnitStatus newStatus,
        SegmentMembership newMembership) {
      this.success = success;
      this.newStatus = newStatus;
      this.newMembership = newMembership;
    }

    public boolean success() {
      return success;
    }

    public SegmentUnitStatus getNewStatus() {
      return newStatus;
    }

    public void setNewStatus(SegmentUnitStatus newStatus) {
      this.newStatus = newStatus;
    }

    public SegmentMembership getNewMembership() {
      return newMembership;
    }
  }
}
