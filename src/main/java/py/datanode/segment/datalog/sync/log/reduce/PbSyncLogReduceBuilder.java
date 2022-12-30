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

package py.datanode.segment.datalog.sync.log.reduce;

import com.google.protobuf.Message;
import io.netty.util.Timeout;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.instance.InstanceId;

public abstract class PbSyncLogReduceBuilder<U extends Message, M extends Message> implements
    ReduceBuilder<U, M> {
  private static final Logger logger = LoggerFactory.getLogger(PbSyncLogReduceBuilder.class);
  private static final AtomicInteger reduceBuilderIdGenerate = new AtomicInteger();
  private final int reduceBuilderId = reduceBuilderIdGenerate.incrementAndGet();
  private final InstanceId destination;
  private final int maxReduceCacheLength;
  private int currentCacheLength = 0;
  private Timeout timeout;
  private AtomicBoolean hasDone = new AtomicBoolean(false);

  public PbSyncLogReduceBuilder(InstanceId destination, int maxReduceCacheLength) {
    this.destination = destination;
    this.maxReduceCacheLength = maxReduceCacheLength;
  }

  @Override
  public synchronized boolean submitUnit(U unit) {
    if (hasDone()) {
      return false;
    } else if (fillMessage(unit.getSerializedSize())) {
      enqueue(unit);
      return true;
    } else {
      return false;
    }
  }

  @Override
  public synchronized boolean submitUnit(U unit, boolean force) {
    if (force) {
      currentCacheLength += unit.getSerializedSize();
      logger.debug(
          "reduce builder fill one message, length {}, " 
              + "current cache length {}, reduce builder id {}",
          unit.getSerializedSize(), currentCacheLength, reduceBuilderId);

      enqueue(unit);
      return true;
    }

    return submitUnit(unit);
  }

  public InstanceId getDestination() {
    return destination;
  }

  public boolean setTimeout(Timeout timeout) {
    this.timeout = timeout;
    return !hasDone.get();
  }

  public Timeout getTimeout() {
    return timeout;
  }

  public void setHasDone() {
    hasDone.set(true);
    if (null != timeout) {
      timeout.cancel();
    }
  }

  public boolean hasDone() {
    return hasDone.get();
  }

  public boolean overflow() {
    return currentCacheLength >= maxReduceCacheLength;
  }

  protected synchronized boolean fillMessage(int length) {
    if (overflow()) {
      logger.info(
          "reduce builder has over flow, "
              + "current cache length {}, max reduce cache length {}, reduce builder id {}",
          currentCacheLength, maxReduceCacheLength, reduceBuilderId);
      return false;
    }

    if (currentCacheLength + length > maxReduceCacheLength) {
      logger.info(
          "reduce builder has fill up, "
              + "current cache length {}, max reduce cache length {}, "
              + "fill message length {}, reduce builder id {}",
          currentCacheLength, maxReduceCacheLength, length, reduceBuilderId);
      return false;
    }

    currentCacheLength += length;
    logger.debug(
        "reduce builder fill one message, length {}, current cache length {}, reduce builder id {}",
        length, currentCacheLength, reduceBuilderId);

    return true;
  }

  private synchronized void setExpired() {
    logger.info("reduce builder has expired request {}", this);
    currentCacheLength = maxReduceCacheLength;
  }

  @Override
  public synchronized M expiredBuild() {
    setExpired();
    return build();
  }

  abstract void enqueue(U unit);

  @Override
  public String toString() {
    return "PBSyncLogReduceBuilder{"
        + "reduceBuilderId=" + reduceBuilderId
        + ", destination=" + destination
        + ", maxReduceCacheLength=" + maxReduceCacheLength
        + ", currentCacheLength=" + currentCacheLength
        + ", timeout=" + timeout
        + ", hasDone=" + hasDone
        + '}';
  }
}
