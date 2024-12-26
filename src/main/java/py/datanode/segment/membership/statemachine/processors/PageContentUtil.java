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
