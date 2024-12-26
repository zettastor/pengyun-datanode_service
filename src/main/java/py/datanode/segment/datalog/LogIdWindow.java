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
