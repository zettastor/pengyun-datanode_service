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
