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

package py.datanode.segment.datalog.plal.engine;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.app.context.AppContext;
import py.archive.page.PageAddress;
import py.archive.segment.SegId;
import py.common.NamedThreadFactory;
import py.datanode.page.impl.BogusPageAddress;
import py.datanode.page.impl.PageAddressGenerator;
import py.datanode.segment.SegmentUnit;
import py.datanode.segment.datalog.MutationLogEntry;
import py.datanode.service.DataNodeServiceImpl;
import py.storage.Storage;

public class PlalEngine {
  private static final Logger logger = LoggerFactory.getLogger(PlalEngine.class);
  private static final int DEFAULT_WAIT_TIME_MS = 1000;
  private static final int COUNT_LOG_PERIOD = 1000;
  private final AtomicLong logCounter = new AtomicLong(0);
  private Map<Storage, PlalWorker> segIdMappedToStorage;
  private volatile boolean stopEngine = false;
  private DataNodeServiceImpl service;
  private ThreadPoolExecutor failToPersistThreadPoolExecutor;

  private AtomicBoolean pauseFlag = new AtomicBoolean(true);

  public PlalEngine() {
    segIdMappedToStorage = new ConcurrentHashMap<Storage, PlalWorker>();
    failToPersistThreadPoolExecutor = new ThreadPoolExecutor(1, 1, 60, TimeUnit.SECONDS,
        new LinkedBlockingQueue<>(), new NamedThreadFactory("page-failToPersist"));
  }

  public void putLogs(SegId segId, List<MutationLogEntry> logs) {
    for (MutationLogEntry logEntry : logs) {
      putLog(segId, logEntry);
    }
  }

  public boolean putLog(PageAddress pageAddress) {
    if (stopEngine) {
      logger
          .warn("engine has been stopped. Can't put a page address {} to the unused", pageAddress);
      return false;
    }
    if (pauseFlag.get() && (0 == logCounter.getAndIncrement() % COUNT_LOG_PERIOD)) {
      logger.warn("engine has been paused. Can't put a page address {} to the unused", pageAddress);
      return false;
    }

    logger.debug("Putting page {} to the plalworker", pageAddress);
    try {
      if (pageAddress instanceof BogusPageAddress) {
        return false;
      }

      Storage storage = pageAddress.getStorage();
      PlalWorker worker = getPlalWorker(storage);
      return worker.add(pageAddress);
    } catch (Exception e) {
      logger.error("caught an exception", e);
      throw e;
    }
  }
  

  public boolean putLog(SegId segId, MutationLogEntry log) {
    if (stopEngine) {
      logger.warn("engine has been stopped. Can't put a log {} to the unused", log);
      return false;
    }
    if (pauseFlag.get() && (0 == logCounter.getAndIncrement() % COUNT_LOG_PERIOD)) {
      logger.warn("engine has been paused. Can't put a page address {} to the unused", log);
      return false;
    }
    int pageSize = service.getCfg().getPageSize();
    SegmentUnit segUnit = service.getSegmentUnitManager().get(segId);

    if (segUnit != null) {
      if (log == null || !log.canBeApplied()) {
        return false;
      }

      int pageIndex = PageAddressGenerator.calculatePageIndex(log.getOffset(), pageSize);
      PageAddress pageAddress = segUnit.getLogicalPageAddressToApplyLog(pageIndex);
      PlalWorker worker = getPlalWorker(pageAddress.getStorage());
      return worker.add(pageAddress);
    }

    return false;
  }

  public boolean checkSegmentUnitStopped(SegId segId) {
    Storage storage = service.getSegmentUnitManager().get(segId).getArchive().getStorage();
    PlalWorker worker = getPlalWorker(storage);
    CheckPageAddress address = new CheckPageAddress(segId, storage);
    worker.add(new CheckPageAddress(segId, storage));
    return address.waitFor(DEFAULT_WAIT_TIME_MS, TimeUnit.MILLISECONDS);
  }

  private PlalWorker getPlalWorker(Storage storage) {
    PlalWorker worker = segIdMappedToStorage.get(storage);
    if (worker == null) {
      synchronized (segIdMappedToStorage) {
        worker = segIdMappedToStorage.get(storage);
        if (worker == null) {
          worker = new PlalWorker(service, storage, failToPersistThreadPoolExecutor);
          worker.start();
          segIdMappedToStorage.put(storage, worker);
        }
      }
    }

    return worker;
  }

  public void pausePlalWorker() {
    pauseFlag.set(true);
  }

  public void restartPlalWorker() {
    pauseFlag.set(false);
  }

  public void stop() {
    AppContext myContext = null;
    if (service != null) {
      myContext = service.getContext();
    }
    logger.info("stopping plal engine for instance {}", myContext);
    stopEngine = true;
    Collection<PlalWorker> workers = segIdMappedToStorage.values();
    for (PlalWorker worker : workers) {
      try {
        worker.stop();
      } catch (Exception e) {
        logger.warn("stop worker {} failure", worker);
      }
    }

    failToPersistThreadPoolExecutor.shutdown();
    try {
      failToPersistThreadPoolExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);

    } catch (InterruptedException e) {
      logger.error("stop pool executor error");
    }
    logger.info("stopped plal engine for instance {}", myContext);
  }

  public void start() {
    failToPersistThreadPoolExecutor = new ThreadPoolExecutor(1, 1, 60, TimeUnit.SECONDS,
        new LinkedBlockingQueue<>(), new NamedThreadFactory("page-failToPersist"));
    stopEngine = false;
  }

  public DataNodeServiceImpl getService() {
    return service;
  }

  public void setService(DataNodeServiceImpl service) {
    this.service = service;
  }

  public int getMaxNumberOfPagesToApplyPerDrive() {
    return service.getCfg().getMaxNumberOfPagesToApplyPerDrive();
  }

}
