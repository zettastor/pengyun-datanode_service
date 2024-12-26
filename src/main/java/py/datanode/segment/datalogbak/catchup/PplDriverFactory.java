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
import py.archive.segment.SegId;
import py.datanode.configuration.DataNodeConfiguration;
import py.datanode.page.Page;
import py.datanode.page.PageManager;
import py.datanode.segment.SegmentUnit;
import py.datanode.segment.SegmentUnitManager;
import py.datanode.segment.datalog.persist.LogPersister;

public class PplDriverFactory {
  private static final Logger logger = LoggerFactory.getLogger(PplDriverFactory.class);
  private final LogPersister logPersister;
  private final DataNodeConfiguration cfg;
  private final PageManager<Page> pageManager;
  private final SegmentUnitManager segmentUnitManager;

  public PplDriverFactory(SegmentUnitManager segmentUnitManager, PageManager<Page> pageManager,
      LogPersister logPersister, DataNodeConfiguration cfg) {
    this.pageManager = pageManager;
    this.logPersister = logPersister;
    this.segmentUnitManager = segmentUnitManager;

    this.cfg = cfg;
  }

  public ChainedLogDriver generate(CatchupLogContext context) {
    SegmentUnit segmentUnit = context.getSegmentUnit();
    SegId segId = context.getSegId();
    if (segmentUnit != null) {
      return new PplDriver(segmentUnit.getSegmentLogMetadata(), segmentUnit,
          segmentUnitManager, pageManager, logPersister, cfg);
    } else {
      logger.error("Can't find the segment unit for segId: {} ", segId);
      return null;
    }
  }
}
