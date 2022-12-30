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

package py.datanode.segment.copy.unused;

import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.page.PageAddress;
import py.archive.segment.CloneStatus;
import py.archive.segment.CloneType;
import py.archive.segment.SegId;
import py.common.lock.HashLock;
import py.common.lock.HashLockImpl;
import py.common.struct.Pair;
import py.datanode.configuration.DataNodeConfiguration;
import py.datanode.exception.CopyPageExistingException;
import py.datanode.page.Page;
import py.datanode.page.PageContext;
import py.datanode.page.PageManager;
import py.datanode.page.TaskType;
import py.datanode.page.context.PageContextFactory;
import py.datanode.page.context.SinglePageContextFactory;
import py.datanode.page.impl.GarbagePageAddress;
import py.datanode.page.impl.PageAddressGenerator;
import py.datanode.segment.SegmentUnit;
import py.datanode.segment.copy.CopyPage;
import py.datanode.segment.datalog.SegmentLogMetadata;
import py.datanode.service.io.throttle.CopyPageSampleInfo;
import py.datanode.service.io.throttle.IoThrottleManager;
import py.engine.DelayedTask;
import py.engine.DelayedTaskEngine;
import py.engine.Result;
import py.engine.ResultImpl;
import py.engine.SingleTaskEngine;
import py.engine.Task;
import py.engine.TaskEngine;
import py.engine.TaskListener;
import py.netty.client.GenericAsyncClientFactory;
import py.netty.datanode.AsyncDataNode;

public class CopyPageContextManager extends DelayedTaskEngine implements
    TaskListener<CopyPageResult> {
  private final Logger logger = LoggerFactory.getLogger(CopyPageContextManager.class);

  private final TaskEngine taskEngine;
  private final DataNodeConfiguration cfg;
  private final PageManager<Page> pageManager;
  private final GenericAsyncClientFactory<AsyncDataNode.AsyncIface> clientFactory;
  private final int perCopyCount;
  private final int delayForPlalMove = 100;
  
  private final Multimap<SegId, CopyPageTask> mapSegIdToTask;
  private IoThrottleManager ioThrottleManager;
  private HashLock<SegId> lock = new HashLockImpl<>();
  
  
  public CopyPageContextManager(TaskEngine taskEngine, DataNodeConfiguration cfg,
      PageManager<Page> pageManager,
      GenericAsyncClientFactory<AsyncDataNode.AsyncIface> clientFactory) {
    setPrefix("delay-primary-copy-page");
    this.taskEngine = new SingleTaskEngine();
    this.taskEngine.start();
    this.cfg = cfg;
    this.pageManager = pageManager;
    this.mapSegIdToTask = Multimaps.synchronizedMultimap(LinkedHashMultimap.create());
    this.clientFactory = clientFactory;
    this.perCopyCount = cfg.getPageCountInRawChunk();
  }

  public IoThrottleManager getIoThrottleManager() {
    return ioThrottleManager;
  }

  public void setIoThrottleManager(IoThrottleManager ioThrottleManager) {
    this.ioThrottleManager = ioThrottleManager;
  }

  public int size() {
    return mapSegIdToTask.size();
  }

  public void start() {
    super.start();
  }

  public void stop() {
    try {
      this.taskEngine.stop();
      super.stop();
    } catch (Exception e) {
      logger.warn("caught an exception", e);
    }
  }

  /**
   * create a new copy page task and start to drive the task engine.
   */
  public Pair<Boolean, Integer> register(CopyPageContext copyPageContext)
      throws CopyPageExistingException {
    SegmentUnit segmentUnit = copyPageContext.getSegmentUnit();
    SegId segId = segmentUnit.getSegId();
    CopyPageTask copyPageTask = null;

    try {
      lock.lock(segId);
    } catch (InterruptedException e) {
      logger.error("lockError");
      return new Pair(false, CopyPageSampleInfo.DEFAULT_MIN_FINISH_TIME_MS);
    }
    try {
      if (mapSegIdToTask.get(segId).size() > 0) {
        if (copyPageContext.getSessionId() == mapSegIdToTask.get(segId).iterator().next()
            .getSessionId()) {
          logger.warn("resend two times, old task={}, new={}", mapSegIdToTask.get(segId),
              copyPageContext);
          return new Pair(false, CopyPageSampleInfo.DEFAULT_MIN_FINISH_TIME_MS);
        } else {
          throw new CopyPageExistingException();
        }
      }
      Pair<Boolean, Integer> resultPair = ioThrottleManager
          .register(segmentUnit.getArchive(), segId, 0, copyPageContext.getSessionId());
      if (resultPair.getFirst()) {
        for (int j = 0; j < cfg.getConcurrentCopyPageTaskCount(); j++) {
          CopyPageContext newContext = copyPageContext.duplicate();
          long taskId = j;
          copyPageTask = new CopyPageTask(this, newContext, clientFactory, pageManager,
              ioThrottleManager,
              taskId);
          logger.warn(
              "success to register a new push data context {} "
                  + " archive {} wrapper={}",
              newContext, segmentUnit.getArchive().getArchiveId(), copyPageTask);
          mapSegIdToTask.put(segId, copyPageTask);

          taskEngine.drive(copyPageTask);
        }
      }
      return resultPair;
    } finally {
      lock.unlock(segId);
    }

  }

  @Override
  public void response(CopyPageResult result) {
    logger.debug("get a response from secondary and result={}", result);
    final CopyPageTask copyPageTask = result.getCopyPageTask();

    SegmentUnit segmentUnit = copyPageTask.getCopyPageContext().getSegmentUnit();
    SegId segId = segmentUnit.getSegId();

    ioThrottleManager.addAlready(segId, 0);
   
    if (!result.isSuccess()) {
      logger.warn("fail to push copy page unit={}, result={}", copyPageTask.getCopyPageContext(),
          result);
      copyPageTask.getCopyPageContext().incrementErrorCount();
    }

    if (copyPageTask.isCancel()) {
      while (true) {
        try {
          lock.lock(segId);
          break;
        } catch (InterruptedException e) {
          logger.error("lock error");
        }

      }
      try {
        mapSegIdToTask.remove(copyPageTask.getSegId(), copyPageTask);
        int count = mapSegIdToTask.get(copyPageTask.getSegId()).size();
        logger.warn("fail to copy page for task={}, left task count {}", copyPageTask, count);
        if (count == 0) {
          ioThrottleManager.finish(segmentUnit.getArchive(), segId);
        }
        return;
      } finally {
        lock.unlock(segId);
      }

    }
    
    if (result.isDone()) {
      while (true) {
        try {
          lock.lock(segId);
          break;
        } catch (InterruptedException e) {
          logger.error("lock error");
        }
      }
      try {
        mapSegIdToTask.remove(copyPageTask.getSegId(), copyPageTask);
        int count = mapSegIdToTask.get(copyPageTask.getSegId()).size();
        logger.warn("success to copy page for task={}, left task count {}", copyPageTask, count);
       
        if (count == 0) {
          ioThrottleManager.finish(segmentUnit.getArchive(), segId);
        }
        return;
      } finally {
        lock.unlock(segId);
      }

    }

    copyPageTask.setToken(perCopyCount);
    List<CopyPage> copyPages = copyPageTask.getCopyPageContext().getCopyPageUnitToSend();
    if (copyPages.size() > 0) {
      copyPageTask.setCopyPageToSend(copyPages);
      drive(new DelayedCopyPageTask(copyPageTask, null, taskEngine, ioThrottleManager, segId));
      return;
    }

    List<CopyPage> copyPageUnitToRead = copyPageTask.getCopyPageContext().getCopyPageUnitToRead();
    PageContext<Page> pageContext;
    SegmentLogMetadata segmentLogMetadata = segmentUnit.getSegmentLogMetadata();
    long lalId = segmentLogMetadata.getLalId();

    logger.info("secondary plal id: {} for segId: {}", lalId, segId);
    Map<PageAddress, CopyPage> physicalPageAddressPageCopyMap = copyPageTask
        .getMapPageAddressToPage();
    List<PageContext<Page>> pageContexts = new ArrayList<>();
    List<PageContext<Page>> garbageContexts = new ArrayList<>();
    PageContextFactory<Page> factory = SinglePageContextFactory.getInstance();
    int pageCount = 0;
    int garbageCount = 0;
    for (CopyPage copyPage : copyPageUnitToRead) {
      logger.debug("copy page: {}", copyPage);
      if (lalId < copyPage.getMaxLogId()) {
        logger.warn("primary plal id={} less than the secondary last log id={}", lalId,
            copyPage.getMaxLogId());
        continue;
      }

      CopyPage.PageNode pageNode = copyPage.content();

      CopyPage old = physicalPageAddressPageCopyMap.put(pageNode.physicalPageAddress, copyPage);
      if (old != null) {
        logger.error("current content {}, old copy page: {}, new copy page: {}, copy page task={}",
            pageNode,
            old, copyPage, copyPageTask);
        Validate.isTrue(false);
      }

      int pageIndex = PageAddressGenerator.calculatePageIndex(copyPage.getLogicalPageAddress());

      pageContext = factory
          .generateAsyncCheckoutContext(pageNode.physicalPageAddress, TaskType.CHECK_OUT_FOR_READ,
              donePageContext -> drive(
                  new DelayedCopyPageTask(copyPageTask, donePageContext, taskEngine,
                      ioThrottleManager,
                      segId)),
              cfg.getDefaultPageRequestTimeoutMs());
      if (GarbagePageAddress.isGarbagePageAddress(pageNode.physicalPageAddress)) {
        logger.info("no need submit to page system due to GarbagePageAddress {}", copyPage);
        garbageContexts.add(pageContext);
        garbageCount++;
      } else {
        logger.debug("submit to page system for address={}, copy page={}",
            pageNode.physicalPageAddress,
            copyPage);
        pageContexts.add(pageContext);
        pageCount++;
      }
    }

    if (pageCount == 0 && garbageCount == 0) {
      int delay = 0;
     
      if (copyPageUnitToRead.size() == 0
          && copyPageTask.getCopyPageContext().getCopyPageUnitBitmap() != null) {
        logger.info("bitmap is done, for segId={} task = {}", segId, copyPageTask);
        Validate.isTrue(copyPageTask.getCopyPageContext().getCopyPageUnitBitmap().isFull());
      } else {
        logger.warn("delay for plal move {}, {}", copyPageTask, copyPages);
        delay = delayForPlalMove;
      }
      drive(new DelayedCopyPageTask(delay, copyPageTask, null, taskEngine, ioThrottleManager,
          segId));
    } else {
      if (pageCount > 0) {
        pageManager.checkout(factory.generatePageContextWrapper(pageContexts));
      }

      if (garbageCount > 0) {
        for (PageContext garbagePageContext : garbageContexts) {
          drive(new DelayedCopyPageTask(copyPageTask, garbagePageContext, taskEngine,
              ioThrottleManager,
              segId));
        }
      }
    }
  }

  public boolean isOver(SegId segId) {
    return !mapSegIdToTask.containsKey(segId);
  }

}
