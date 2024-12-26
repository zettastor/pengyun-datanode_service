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

package py.datanode.segment.copy.unused;

import java.nio.ByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.page.PageAddress;
import py.archive.segment.SegId;
import py.datanode.page.Page;
import py.datanode.page.PageContext;
import py.datanode.page.impl.GarbagePageAddress;
import py.datanode.segment.copy.CopyPage;
import py.datanode.service.io.throttle.IoThrottleManager;
import py.engine.DelayedTask;
import py.engine.Result;
import py.engine.ResultImpl;
import py.engine.TaskEngine;

public class DelayedCopyPageTask extends DelayedTask {
  private static final Logger logger = LoggerFactory
      .getLogger(DelayedCopyPageTask.class.getSimpleName());
  private final CopyPageTask copyPageTask;
  private final PageContext<Page> pageContext;
  private final TaskEngine taskEngine;
  private final IoThrottleManager ioThrottleManager;
  private final SegId segId;

  public DelayedCopyPageTask(CopyPageTask copyPageTask, PageContext<Page> pageContext,
      TaskEngine taskEngine,
      IoThrottleManager ioThrottleManager, SegId segId) {
    this(0, copyPageTask, pageContext, taskEngine, ioThrottleManager, segId);
  }

  public DelayedCopyPageTask(int delayMs, CopyPageTask copyPageTask,
      PageContext<Page> pageContext,
      TaskEngine taskEngine, IoThrottleManager ioThrottleManager, SegId segId) {
    super(delayMs);
    this.copyPageTask = copyPageTask;
    this.pageContext = pageContext;
    this.taskEngine = taskEngine;
    this.ioThrottleManager = ioThrottleManager;
    this.segId = segId;
  }

  @Override
  public Result work() {
    try {
      internalDoWork();
    } catch (Throwable e) {
      logger.error("caugth an exception", e);
      copyPageTask.cancel();
      copyPageTask.getTaskListener()
          .response(new CopyPageResult(copyPageTask, new RuntimeException(e)));
    }
    return ResultImpl.DEFAULT;
  }

  private void internalDoWork() {
    if (pageContext == null) {
      logger.info("no pages to send, task={}", copyPageTask);
      taskEngine.drive(copyPageTask);
      return;
    }

    PageAddress physicalPageAddress = pageContext.getPageAddressForIo();
    CopyPage copyPage = copyPageTask.getMapPageAddressToPage().remove(physicalPageAddress);

    CopyPageContext copyPageContext = copyPageTask.getCopyPageContext();

    if (GarbagePageAddress.isGarbagePageAddress(physicalPageAddress)) {
      logger.warn("no need to load page due to it is a garbage pageaddress {}, copypage {}, ",
          physicalPageAddress, copyPage);

      copyPage.content().buffer = copyPageContext.getPageByteBuffer(copyPage.getPageIndex());
    } else {
      try {
        if (!pageContext.isSuccess()) {
          logger.warn("can not load the page from page system, context={}, copy page context={}",
              pageContext,
              copyPageTask.getCopyPageContext());
          copyPageTask.getCopyPageContext().incrementErrorCount();
        } else {
          logger.debug("load a page from page system, context={}", pageContext);
          Page page = pageContext.getPage();
          ByteBuffer buffer = copyPageContext.getPageByteBuffer(copyPage.getPageIndex());
          page.getData(0, buffer);
          doneWithLoadPage(copyPage, buffer, copyPageContext);
        }
      } catch (Exception e) {
        logger.warn("fail to load page:{}, task={}, CopyPage={}", physicalPageAddress, copyPageTask,
            copyPage, e);
        copyPageTask.cancel();
      } finally {
        pageContext.updateSegId(segId);
        copyPageTask.getPageManager().checkin(pageContext);
      }
    }

    if (copyPageTask.getMapPageAddressToPage().size() == 0) {
      logger.info("all pages are checkout for task {}", copyPageTask);
      if (copyPageTask.isCancel()) {
        copyPageTask.getTaskListener().response(new CopyPageResult(copyPageTask, null));
      } else {
        copyPageTask.setCopyPageToSend(copyPageContext.getCopyPageUnitToSend());
        taskEngine.drive(copyPageTask);
      }
    } else {
      logger.debug("there are still some pages to be loaded from page system, map={}",
          copyPageTask.getMapPageAddressToPage());
    }
  }

  public void doneWithLoadPage(CopyPage copyPageUnit, ByteBuffer buffer,
      CopyPageContext copyPageContext) {
    CopyPage.PageNode pageNode = copyPageUnit.content();
    long lastLogId = copyPageContext.getSegmentUnit().getSegmentLogMetadata()
        .getContinuousAppliedLogId(copyPageUnit.getLogicalPageAddress());
    if (copyPageUnit.getMaxLogId() < lastLogId) {
      copyPageUnit.setMaxLogId(lastLogId);
    }
    pageNode.buffer = buffer;
  }

}
