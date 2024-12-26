/**
* Copyright (C) 2013-2024 Nanjing Pengyun Network Technology Co., Ltd.
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/ 

package py.datanode.segment.datalogbak.catchup;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.segment.recurring.SegmentUnitProcessor;
import py.archive.segment.recurring.SegmentUnitProcessorFactory;
import py.archive.segment.recurring.SegmentUnitTaskContext;
import py.client.thrift.GenericThriftClientFactory;
import py.datanode.archive.RawArchiveManager;
import py.datanode.client.DataNodeServiceAsyncClientWrapper;
import py.datanode.configuration.DataNodeConfiguration;
import py.datanode.page.Page;
import py.datanode.page.PageManager;
import py.datanode.segment.SegmentUnitManager;
import py.datanode.segment.datalog.persist.LogPersister;
import py.datanode.segment.datalog.plal.engine.PlalEngine;
import py.instance.InstanceId;
import py.instance.InstanceStore;
import py.thrift.datanode.service.DataNodeService.Iface;

public class SegmentUnitDataLogProcessorFactory implements SegmentUnitProcessorFactory {
  private static final Logger logger = LoggerFactory
      .getLogger(SegmentUnitDataLogProcessorFactory.class);
  private final PclDriverFactory pclDriverFactory;

  private final PplDriverFactory pplDriverFactory;

  public SegmentUnitDataLogProcessorFactory(RawArchiveManager archiveManager,
      LogPersister logPersister,
      PageManager<Page> pageManager, GenericThriftClientFactory<Iface> dataNodeClientFactory,
      InstanceStore instanceStore,
      DataNodeConfiguration cfg, InstanceId myself, PlalEngine plalEngine,
      SegmentUnitManager segmentUnitManager,
      DataNodeServiceAsyncClientWrapper dataNodeAsyncClient) {
    pclDriverFactory = new PclDriverFactory(archiveManager, dataNodeClientFactory,
        instanceStore, cfg, myself, plalEngine, dataNodeAsyncClient);
    pplDriverFactory = new PplDriverFactory(segmentUnitManager, pageManager, logPersister, cfg);
  }

  @Override
  public SegmentUnitProcessor generate(SegmentUnitTaskContext context) {
    if (!(context instanceof CatchupLogContext)) {
      logger.warn("context : {} does not have a CatchupLogContextKey", context);
      return null;
    }
    CatchupLogContext catchupLogContext = (CatchupLogContext) context;
    CatchupLogContextKey key = catchupLogContext.getKey();

    ChainedLogDriver firstDriver = null;
    switch (key.getLogDriverType()) {
      case PCL:
        firstDriver = pclDriverFactory.generate(catchupLogContext);
        break;
      case PPL:
        firstDriver = pplDriverFactory.generate(catchupLogContext);
        break;
      default:
        logger.error("not supported driver type: {} ", key.getLogDriverType());
        break;
    }
    return new SegmentUnitDataLogProcessor(catchupLogContext, firstDriver);
  }

  public PclDriverFactory getPclDriverFactory() {
    return pclDriverFactory;
  }

  public PplDriverFactory getPplDriverFactory() {
    return pplDriverFactory;
  }
}
