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

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.segment.SegId;
import py.archive.segment.SegmentUnitMetadata;
import py.archive.segment.SegmentUnitStatus;
import py.archive.segment.recurring.SegmentUnitProcessResult;
import py.archive.segment.recurring.SegmentUnitTaskContext;
import py.archive.segment.recurring.SegmentUnitTaskContextFactory;
import py.datanode.segment.SegmentUnit;
import py.datanode.segment.SegmentUnitManager;
import py.datanode.segment.membership.statemachine.StateProcessingContextKey.PrimaryProcessingType;
import py.datanode.segment.membership.statemachine.processors.CopyPagesContext;
import py.datanode.segment.membership.statemachine.processors.JoinGroupContext;
import py.datanode.segment.membership.statemachine.processors.NewPrimaryProposerContext;
import py.datanode.segment.membership.statemachine.processors.StateProcessingContext;

public class StateProcessingContextFactory implements SegmentUnitTaskContextFactory {
  private static final Logger logger = LoggerFactory.getLogger(StateProcessingContextFactory.class);
  private final SegmentUnitManager segmentUnitManager;

  public StateProcessingContextFactory(SegmentUnitManager segmentUnitManager) {
    this.segmentUnitManager = segmentUnitManager;
  }

  @Override
  public List<SegmentUnitTaskContext> generateProcessingContext(SegId segId) {
    List<SegmentUnitTaskContext> contexts = new ArrayList<SegmentUnitTaskContext>(1);
   
   
    SegmentUnit segUnit = segmentUnitManager.get(segId);
    Validate.notNull(segUnit);

    SegmentUnitStatus status = segUnit.getSegmentUnitMetadata().getStatus();
    logger.debug("generate process context: {}", segUnit);

    if (status != SegmentUnitStatus.OFFLINED && status != SegmentUnitStatus.Deleting) {
      contexts.add(new NewPrimaryProposerContext(segId, segUnit));
    } else {
      logger.info("segment unit: {}, status: {} ", segUnit, status);
      contexts
          .add(new StateProcessingContext(new StateProcessingContextKey(segId), status, segUnit));
    }

    return contexts;
  }

  @Override
  public List<SegmentUnitTaskContext> generateProcessingContext(SegmentUnitProcessResult result) {
    List<SegmentUnitTaskContext> contexts = new ArrayList<SegmentUnitTaskContext>();
    
    StateProcessingResult stateProcessingResult = null;
    if (!(result instanceof StateProcessingResult)) {
      logger.info("the context is not the type of StatProcessingResult {}", result);
      return contexts;
    }

    stateProcessingResult = (StateProcessingResult) result;
    StateProcessingContext context = stateProcessingResult.getContext();

    if (context.isAbandonedTask()) {
      logger.debug("context {} is going to be abandoned", context);
      context.updateDelay(10);
     
      contexts.add(context);
      return contexts;
    }

    SegId segId = context.getKey().getSegId();
    SegmentUnitStatus segmentUnitStatusWhenContextCreated = context.getStatus();
    SegmentUnitStatus newStatus = stateProcessingResult.getNewStatus();
    logger.debug("The new status of segment {} is {} and the old status is {} ", segId, newStatus,
        segmentUnitStatusWhenContextCreated);

    if (!stateProcessingResult.executionSuccess()) {
      context.incFailureTimes();
    }

    if (segmentUnitStatusWhenContextCreated == newStatus) {
      context.updateDelay(result.getDelayToExecute());
     
     
      contexts.add(context);
      logger.debug("the returned contexts {}", contexts);
      return contexts;
    }

    SegmentUnitMetadata unitMetadata = segmentUnitManager.get(segId).getSegmentUnitMetadata();
    if (segmentUnitStatusWhenContextCreated == SegmentUnitStatus.Start && unitMetadata
        .getMigrationStatus()
        .isMigrating()) {
      logger.warn("current status is Start for segId={}, so go on with context={}", segId, context);
      context.updateDelay(result.getDelayToExecute());
      contexts.add(context);
      return contexts;
    }

    if (newStatus == SegmentUnitStatus.Primary) {
     
     
     
      StateProcessingContextKey newKey = new StateProcessingContextKey(segId,
          PrimaryProcessingType.Janitor.name());
      StateProcessingContext newJanitorContext = new StateProcessingContext(newKey,
          SegmentUnitStatus.Primary,
          context.getSegmentUnit());
     
      newJanitorContext.updateDelayWithForce(0);
      contexts.add(newJanitorContext);

      if (segId.getIndex() == 0) {
        newKey = new StateProcessingContextKey(segId,
            PrimaryProcessingType.VolumeMetadataDuplicator.name());
        StateProcessingContext newVmdContext = new StateProcessingContext(newKey,
            SegmentUnitStatus.Primary,
            context.getSegmentUnit());
        newVmdContext.updateDelayWithForce(0);
        contexts.add(newVmdContext);
      }

      newKey = new StateProcessingContextKey(segId, PrimaryProcessingType.ExpirationChecker.name());
      StateProcessingContext newEcContext = new StateProcessingContext(newKey,
          SegmentUnitStatus.Primary,
          context.getSegmentUnit());

      newEcContext.updateDelayWithForce(0);
      contexts.add(newEcContext);
    } else {
      if (segmentUnitStatusWhenContextCreated == SegmentUnitStatus.Primary) {
       
        Validate.notNull(context.getKey().getStateProcessingType());
       
       
       
      }
     
     
      StateProcessingContext newContext = generateContextByNewStatus(segId, newStatus,
          context.getSegmentUnit());
      newContext.updateDelay(result.getDelayToExecute());
      contexts.add(newContext);
    }

    context.setAbandonedTask(true);
    context.updateDelay(10);
   
    contexts.add(context);
    logger.debug("the returned contexts {}", contexts);
    return contexts;
  }

  private StateProcessingContext generateContextByNewStatus(SegId segId,
      SegmentUnitStatus newStatus,
      SegmentUnit segmentUnit) {
    switch (newStatus) {
      case PreSecondary:
        return new CopyPagesContext(segId, segmentUnit);
      case SecondaryApplicant:
        return new JoinGroupContext(segId, segmentUnit);
      case Start:
        return new NewPrimaryProposerContext(segId, segmentUnit);
      default:
        return new StateProcessingContext(
            new StateProcessingContextKey(segId, newStatus.toString()), newStatus, segmentUnit);
    }
  }
}
