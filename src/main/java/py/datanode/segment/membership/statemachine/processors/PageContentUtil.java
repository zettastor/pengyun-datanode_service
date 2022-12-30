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

import java.nio.ByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.page.PageAddress;
import py.datanode.page.Page;
import py.datanode.page.PageContext;
import py.datanode.page.PageManager;
import py.datanode.segment.SegmentUnit;
import py.thrift.datanode.service.DataPageThrift;

public class PageContentUtil {
  private static final Logger logger = LoggerFactory.getLogger(PageContentUtil.class);

  /**
   * Replace the page with new data. This may happen in copy page or clone volume.
   *
   */
  public static void replacePageWithNewContext(PageManager<Page> pageManager,
      PageAddress pageAddress,
      DataPageThrift pageData) throws Exception {
    logger.debug("Putting to the page {} with {}", pageAddress, pageData);
    PageContext<Page> pageContext = pageManager.checkoutForInternalCorrection(pageAddress);
    if (!pageContext.isSuccess()) {
      logger.error("can't write the data to the page: {}", pageAddress, pageContext.getCause());
      throw pageContext.getCause();
    }
    Page page = pageContext.getPage();
    page.write(0, ByteBuffer.wrap(pageData.getData()));
    page.setPageLoaded(true);
    pageContext.updateSegId(pageAddress.getSegId());
    pageManager.checkin(pageContext);
  }

  public static void writeClonedPageToPageSystem(int pageIndex, ByteBuffer dataPage,
      PageContext<Page> pageContext,
      SegmentUnit segUnit, PageManager<Page> pageManager) throws Exception {
    try {
      if (pageContext == null) {
        throw new RuntimeException("page context is null");
      }
      segUnit.setPageHasBeenWritten(pageIndex);
      segUnit.getSegmentUnitMetadata().getBitmap().set(pageIndex, Migration);
      Page page = pageContext.getPage();
      page.write(0, dataPage);
      page.setPageLoaded(true);
    } finally {
      pageContext.updateSegId(segUnit.getSegId());
      pageManager.checkin(pageContext);
    }
  }


  public static boolean isChecksumMatch(ByteBuffer data, long checksum) {
    return true;
  }

}
