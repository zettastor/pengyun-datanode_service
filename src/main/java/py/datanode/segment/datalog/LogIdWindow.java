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

package py.datanode.segment.datalog;

import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogIdWindow {
  private static final Logger logger = LoggerFactory.getLogger(LogIdWindow.class);
  private CircularLongBuffer buffer;
  private int count;
  private int windowSize;
  private int subWindowSize;

  public LogIdWindow(int windowSize, int splitCount) {
    logger.debug("windowSize={}, splitCount={}", windowSize, splitCount);
    this.windowSize = windowSize;
    this.subWindowSize = windowSize / splitCount;
    if (subWindowSize <= 0) {
      throw new IllegalArgumentException("windowSize=" + windowSize + ", splitCount=" + splitCount);
    }
    this.count = 0;
    this.buffer = new CircularLongBuffer(splitCount);
  }

  public synchronized void addId(long id) {
    Validate.isTrue(id >= -1);
    if (count % subWindowSize == 0) {
      buffer.add(id);

      count = 0;
    } else {
      buffer.resetHeadValue(id);
    }

    count++;
  }

  public synchronized void clean() {
    count = 0;
    buffer.reset();
  }

  public long getLeftEndId() {
    if (buffer.isEmpty()) {
      return -1L;
    } else {
      return buffer.getTailValue();
    }
  }

  public long getRightEndId() {
    if (buffer.isEmpty()) {
      return Long.MAX_VALUE;
    } else {
      return buffer.getHeadValue();
    }
  }

  public synchronized boolean withinWindow(long id) {
    Validate.isTrue(id >= LogImage.INVALID_LOG_ID);
    logger.debug("checking id {} whether within the window [{}, {}]", id, getLeftEndId(),
        getRightEndId());

    if (!buffer.isFull()) {
      return true;
    }

    if (id >= getLeftEndId() && id <= getRightEndId()) {
      return true;
    } else {
      return false;
    }
  }

  @Override
  public String toString() {
    return "LogIdWindow [buffer=" + buffer + ", count=" + count + ", windowSize=" + windowSize
        + ", subWindowSize="
        + subWindowSize + "[ " + getLeftEndId() + "-" + getRightEndId() + "]" + "]";
  }

}
