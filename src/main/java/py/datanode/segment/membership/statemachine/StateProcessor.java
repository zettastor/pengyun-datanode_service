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

package py.datanode.segment.membership.statemachine;

import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.app.context.AppContext;
import py.archive.segment.SegmentUnitStatus;
import py.archive.segment.recurring.SegmentUnitProcessResult;
import py.archive.segment.recurring.SegmentUnitProcessor;
import py.archive.segment.recurring.SegmentUnitTaskExecutor;
import py.client.thrift.GenericThriftClientFactory;
import py.common.struct.EndPoint;
import py.datanode.archive.RawArchiveManager;
import py.datanode.configuration.DataNodeConfiguration;
import py.datanode.page.Page;
import py.datanode.page.PageManager;
import py.datanode.segment.SegmentUnitManager;
import py.datanode.segment.datalog.MutationLogManager;
import py.datanode.segment.datalog.persist.LogPersister;
import py.datanode.segment.datalog.plal.engine.PlalEngine;
import py.datanode.segment.membership.statemachine.processors.StateProcessingContext;
import py.datanode.service.DataNodeRequestResponseHelper;
import py.exception.GenericThriftClientFactoryException;
import py.infocenter.client.InformationCenterClientFactory;
import py.instance.Instance;
import py.instance.InstanceStore;
import py.instance.PortType;
import py.membership.SegmentMembership;
import py.netty.client.GenericAsyncClientFactory;
import py.netty.datanode.AsyncDataNode;
import py.thrift.datanode.service.DataNodeService;
import py.thrift.datanode.service.DataNodeService.Iface;
import py.thrift.share.SegmentMembershipThrift;

/**
 * Process a ticket according the membership status of the ticket and return a update.
 *
 */
public abstract class StateProcessor extends SegmentUnitProcessor {
  private static final Logger logger = LoggerFactory.getLogger(StateProcessor.class);
  // 60 seconds
  private static final long MAX_BACKOFF_TIME_WHEN_THERE_ARE_FAILURES = 60 * 1000;
  // 1 seconds
  private static final long BACKOFF_UNIT = 1000;
  // 1 seconds
  private static final long ABANDONED_CONTEXT_DELAY = 1000;
  private static final Random randomForDelay = new Random(System.currentTimeMillis());

  private static final Random randomForJitter = new Random(System.currentTimeMillis() + 1000);
  private static final Random randomForJitterIncOrDec = new Random(
      System.currentTimeMillis() + 2000);
  protected SegmentUnitManager segmentUnitManager;
  protected GenericThriftClientFactory<Iface> dataNodeSyncClientFactory;
  protected GenericThriftClientFactory<DataNodeService.AsyncIface> dataNodeAsyncClientFactory;
  protected GenericAsyncClientFactory<AsyncDataNode.AsyncIface> dataNodeIoClientFactory;
  protected InformationCenterClientFactory informationCenterClientFactory;
  protected AppContext appContext;
  protected DataNodeConfiguration cfg;
  protected InstanceStore instanceStore;
  protected MutationLogManager mutationLogManager;
  protected LogPersister logPersister;
  protected RawArchiveManager archiveManager;
  protected PageManager<Page> pageManager;
  protected SegmentUnitTaskExecutor catchupLogEngine;
  protected PlalEngine plalEngine;

  public StateProcessor(StateProcessingContext context) {
    super(context);
  }

  protected static long generateDelayWithJitter(long delay) {
    if (delay != 0) {
      int maxJitterValue = (int) ((double) delay * 0.1);
      if (maxJitterValue != 0) {
        int jitter = randomForJitter.nextInt(maxJitterValue);
        if (jitter != 0) {
          if (randomForJitterIncOrDec.nextBoolean()) {
            delay += jitter;
          } else {
            delay -= jitter;
          }
        }
      }
    }
    return delay;
  }

  public static SegmentMembership getHigherMembership(SegmentMembership currentMembership,
      SegmentMembershipThrift returnedMembership) {
    if (returnedMembership != null) {
      SegmentMembership newMembership = DataNodeRequestResponseHelper
          .buildSegmentMembershipFrom(returnedMembership).getSecond();
      if (newMembership.compareTo(currentMembership) > 0) {
        logger.info(
            "the primary has returned a higher membership: {} (previous highest membership: {})",
            returnedMembership, currentMembership);
        return newMembership;
      }
    }
    return null;
  }
  

  public abstract StateProcessingResult process(StateProcessingContext context);

  @Override
  public SegmentUnitProcessResult process() {
    StateProcessingContext context = (StateProcessingContext) getContext();

    try {
      return process(context);
    } catch (Throwable t) {
      logger.error(
          "caught a mystery exception when process " 
              + "the segment unit context {} the context will be resubmit",
          context, t);
      return getFailureResultWithBackoffDelay(context, context.getStatus());
    }
  }

  public long getDefaultBackoffDelay() {
    int failureTimes = getContext().getFailureTimes();

    failureTimes++;
    int baseDelay = 0x1;
    int moves = failureTimes < 10 ? failureTimes : 10;

    return Math
        .min(MAX_BACKOFF_TIME_WHEN_THERE_ARE_FAILURES, BACKOFF_UNIT * (long) (baseDelay << moves));
  }

  public StateProcessingResult getFailureResultWithRandomizedDelay(StateProcessingContext context,
      SegmentUnitStatus currentStatus, long maxDelay) {
    StateProcessingResult result = new StateProcessingResult(context, currentStatus);
    result.setExecutionSuccess(false);
    result.setDelayToExecute(randomForDelay.nextInt((int) maxDelay));
    return result;
  }

  public StateProcessingResult getFailureResultWithZeroDelay(StateProcessingContext context,
      SegmentUnitStatus currentStatus) {
    return getFailureResultWithFixedDelay(context, currentStatus, 10);
  }

  public StateProcessingResult getFailureResultWithBackoffDelay(StateProcessingContext context,
      SegmentUnitStatus currentStatus) {
    return getFailureResultWithFixedDelay(context, currentStatus, getDefaultBackoffDelay());
  }

  public StateProcessingResult getFailureResultWithFixedDelay(StateProcessingContext context,
      SegmentUnitStatus currentStatus, long delay) {
    StateProcessingResult result = new StateProcessingResult(context, currentStatus);
    result.setExecutionSuccess(false);
    result.setDelayToExecute(generateDelayWithJitter(delay));
    return result;
  }

  public StateProcessingResult getAbandonedResult(StateProcessingContext context) {
    context.setAbandonedTask(true);
    StateProcessingResult result = new StateProcessingResult(context, context.getStatus());
    result.setExecutionSuccess(false);
    result.setDelayToExecute(ABANDONED_CONTEXT_DELAY);
    return result;
  }

  public StateProcessingResult getSuccesslResultWithZeroDelay(StateProcessingContext context,
      SegmentUnitStatus currentStatus) {
    return getSuccesslResultWithFixedDelay(context, currentStatus, 0);
  }

  public StateProcessingResult getSuccesslResultWithRandomeDelay(StateProcessingContext context,
      SegmentUnitStatus currentStatus, long maxDelay) {
    return getSuccesslResultWithFixedDelay(context, currentStatus,
        maxDelay <= 0 ? 0 : randomForDelay.nextInt((int) maxDelay));
  }

  public StateProcessingResult getSuccesslResultWithFixedDelay(StateProcessingContext context,
      SegmentUnitStatus currentStatus, long delay) {
    StateProcessingResult result = new StateProcessingResult(context, currentStatus);
    result.setExecutionSuccess(true);
    result.setDelayToExecute(generateDelayWithJitter(delay));
    return result;
  }

  public DataNodeService.Iface getClient(EndPoint endpoint, long timeoutInMs)
      throws GenericThriftClientFactoryException {
    return dataNodeSyncClientFactory.generateSyncClient(endpoint, (int) timeoutInMs);
  }

  public AsyncDataNode.AsyncIface getIoClient(Instance instance) {
    EndPoint ioEndPoint = instance.getEndPointByServiceName(PortType.IO);
    if (ioEndPoint == null) {
      ioEndPoint = instance.getEndPoint();
    }
    return dataNodeIoClientFactory.generate(ioEndPoint);
  }

}