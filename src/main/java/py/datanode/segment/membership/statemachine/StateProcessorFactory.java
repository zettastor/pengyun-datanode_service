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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.app.context.AppContext;
import py.archive.segment.recurring.SegmentUnitProcessor;
import py.archive.segment.recurring.SegmentUnitProcessorFactory;
import py.archive.segment.recurring.SegmentUnitTaskContext;
import py.archive.segment.recurring.SegmentUnitTaskExecutor;
import py.archive.segment.recurring.SegmentUnitTaskType;
import py.client.thrift.GenericThriftClientFactory;
import py.datanode.archive.RawArchiveManager;
import py.datanode.configuration.DataNodeConfiguration;
import py.datanode.page.Page;
import py.datanode.page.PageManager;
import py.datanode.segment.SegmentUnitManager;
import py.datanode.segment.datalog.MutationLogManager;
import py.datanode.segment.datalog.persist.LogPersister;
import py.datanode.segment.datalog.plal.engine.PlalEngine;
import py.datanode.segment.membership.statemachine.StateProcessingContextKey.PrimaryProcessingType;
import py.datanode.segment.membership.statemachine.processors.ArbiterProcessor;
import py.datanode.segment.membership.statemachine.processors.CopyPagesFromPrimaryProcessor;
import py.datanode.segment.membership.statemachine.processors.ExpirationChecker;
import py.datanode.segment.membership.statemachine.processors.Janitor;
import py.datanode.segment.membership.statemachine.processors.JoinGroupProcessor;
import py.datanode.segment.membership.statemachine.processors.LogFlushedChecker;
import py.datanode.segment.membership.statemachine.processors.NewPrimaryProposer;
import py.datanode.segment.membership.statemachine.processors.SegmentUnitDeleter;
import py.datanode.segment.membership.statemachine.processors.StateProcessingContext;
import py.datanode.segment.membership.statemachine.processors.StatusWatcher;
import py.datanode.segment.membership.statemachine.processors.VolumeMetadataDuplicator;
import py.datanode.service.io.throttle.IoThrottleManager;
import py.infocenter.client.InformationCenterClientFactory;
import py.instance.InstanceStore;
import py.netty.client.GenericAsyncClientFactory;
import py.netty.datanode.AsyncDataNode;
import py.thrift.datanode.service.DataNodeService;
import py.thrift.datanode.service.DataNodeService.Iface;

public class StateProcessorFactory implements SegmentUnitProcessorFactory {
  private static final Logger logger = LoggerFactory.getLogger(StateProcessorFactory.class);
  private SegmentUnitManager segmentUnitManager;
  private GenericThriftClientFactory<Iface> dataNodeSyncClientFactory;
  private GenericThriftClientFactory<DataNodeService.AsyncIface> dataNodeAsyncClientFactory;
  private GenericAsyncClientFactory<AsyncDataNode.AsyncIface> dataNodeIoClientFactory;
  private InformationCenterClientFactory informationCenterClientFactory;
  private AppContext appContext;
  private DataNodeConfiguration cfg;
  private InstanceStore instanceStore;
  private MutationLogManager mutationLogManager;
  private LogPersister logPersister;
  private RawArchiveManager archiveManager;
  private SegmentUnitTaskExecutor catchupLogEngine;
  private PageManager<Page> pageManager;
  private PlalEngine plalEngine;
  private IoThrottleManager ioThrottleManager;

  public StateProcessorFactory(SegmentUnitManager segmentUnitManager,
      MutationLogManager mutationLogManager,
      LogPersister logPersister, RawArchiveManager archiveManager,
      GenericThriftClientFactory<DataNodeService.Iface> dataNodeClientFactory,
      GenericThriftClientFactory<DataNodeService.AsyncIface> dataNodeAsyncClientFactory,
      GenericAsyncClientFactory<AsyncDataNode.AsyncIface> dataNodeIoClientFactory,
      InformationCenterClientFactory informationCenterClientFactory,
      AppContext appContext,
      DataNodeConfiguration cfg,
      InstanceStore instanceStore,
      SegmentUnitTaskExecutor catchupLogEngine,
      PageManager pageManager,
      PlalEngine plalEngine,
      IoThrottleManager ioThrottleManager) {
    this.segmentUnitManager = segmentUnitManager;
    this.mutationLogManager = mutationLogManager;
    this.logPersister = logPersister;
    this.archiveManager = archiveManager;
    this.dataNodeAsyncClientFactory = dataNodeAsyncClientFactory;
    this.dataNodeSyncClientFactory = dataNodeClientFactory;
    this.informationCenterClientFactory = informationCenterClientFactory;
    this.appContext = appContext;
    this.cfg = cfg;
    this.instanceStore = instanceStore;
    this.catchupLogEngine = catchupLogEngine;
    this.pageManager = pageManager;
    this.plalEngine = plalEngine;
    this.ioThrottleManager = ioThrottleManager;
    this.dataNodeIoClientFactory = dataNodeIoClientFactory;

  }

  @Override
  public SegmentUnitProcessor generate(SegmentUnitTaskContext context) {
    if (!(context instanceof StateProcessingContext)) {
      logger.warn("context :{} is not StateProcessingContext", context);
      return null;
    }

    StateProcessingContext myContext = (StateProcessingContext) context;
    StateProcessingContextKey key = myContext.getKey();
    StateProcessor processor = null;
    switch (myContext.getStatus()) {
      case Start:
        processor = new NewPrimaryProposer(myContext);
        break;
      case SecondaryApplicant:
        logger.debug("[{}]begin to vote or join process, now status:{}",
            myContext.getSegmentUnit().getSegId(), myContext.getStatus());
        processor = new JoinGroupProcessor(myContext);
        break;
      case Primary:
        if (PrimaryProcessingType.Janitor.name().equals(key.getStateProcessingType())) {
          processor = new Janitor(myContext);
        } else if (PrimaryProcessingType.VolumeMetadataDuplicator.name()
            .equals(key.getStateProcessingType())) {
          processor = new VolumeMetadataDuplicator(myContext);
        } else if (PrimaryProcessingType.ExpirationChecker.name()
            .equals(key.getStateProcessingType())) {
          myContext.setType(SegmentUnitTaskType.ExpirationChecker);
          processor = new ExpirationChecker(myContext);
        } else {
          logger.error("don't know how to process the primary status {} ", myContext);
          processor = new StatusWatcher(myContext);
        }
        break;
      case PreArbiter:
      case Arbiter:
        processor = new ArbiterProcessor(myContext);
        break;
      case OFFLINING:
        logger.debug("current archive status is offlining, create an dataLogFlusher");
        processor = new LogFlushedChecker(myContext);
        break;
      case OFFLINED:
        logger.debug("current archive is offlined");
        processor = new StatusWatcher(myContext);
        break;
      case Deleting:
      case Broken:
        processor = new SegmentUnitDeleter(myContext);
        break;
      case Secondary:
      case ModeratorSelected:
      case SecondaryEnrolled:
        myContext.setType(SegmentUnitTaskType.ExpirationChecker);
        processor = new ExpirationChecker(myContext);
        break;
      case PreSecondary:
        processor = new CopyPagesFromPrimaryProcessor(myContext, ioThrottleManager);
        break;
      case PrePrimary:
      case Deleted:
      case Unknown:
        logger.info("the unit is at some status that needs to be watched {} ", myContext);
        processor = new StatusWatcher(myContext);
        break;
      default:
        logger.info("{}  doesn't have a right type to generate a processor for it", myContext);
        processor = new StatusWatcher(myContext);
        break;
    }

    processor.segmentUnitManager = segmentUnitManager;
    processor.mutationLogManager = mutationLogManager;
    processor.logPersister = logPersister;
    processor.archiveManager = archiveManager;
    processor.dataNodeAsyncClientFactory = dataNodeAsyncClientFactory;
    processor.dataNodeSyncClientFactory = dataNodeSyncClientFactory;
    processor.dataNodeIoClientFactory = dataNodeIoClientFactory;
    processor.informationCenterClientFactory = informationCenterClientFactory;
    processor.appContext = appContext;
    processor.cfg = cfg;
    processor.instanceStore = instanceStore;
    processor.catchupLogEngine = catchupLogEngine;
    processor.pageManager = pageManager;
    processor.plalEngine = plalEngine;

    return processor;

  }
}
