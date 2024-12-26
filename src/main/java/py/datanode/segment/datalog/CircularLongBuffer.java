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

import javax.annotation.concurrent.NotThreadSafe;
import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@NotThreadSafe
public class CircularLongBuffer {
  private static final Logger logger = LoggerFactory.getLogger(CircularLongBuffer.class);

  private final long[] buffer;
  private int size;
  private int nextPosition;

  public CircularLongBuffer(int bufferSize) {
    Validate.isTrue(bufferSize > 0);
    buffer = new long[bufferSize];
    size = 0;
    nextPosition = 0;
  }

  public boolean isEmpty() {
    if (size == 0) {
      Validate.isTrue(nextPosition == 0);
      return true;
    } else {
      return false;
    }
  }

  public boolean isFull() {
    return size == buffer.length;
  }

  public int size() {
    return size;
  }

  public void reset() {
    size = 0;
    nextPosition = 0;
  }

  public void add(long value) {
    logger.trace("current position={}, size={}, new value={}", nextPosition, size, value);
    buffer[nextPosition] = value;
    nextPosition = (++nextPosition) % buffer.length;
    if (size < buffer.length) {
      size++;
    }
  }

  public long getTailValue() {
    if (isEmpty()) {
      throw new IllegalArgumentException(
          "get tail, circle buffer is empty, position=" + nextPosition);
    }
    if (size < buffer.length) {
      return buffer[0];
    } else {
      return buffer[nextPosition];
    }
  }

  public long getHeadValue() {
    if (isEmpty()) {
      throw new IllegalArgumentException(
          "get head, circle buffer is empty, position=" + nextPosition);
    }

    return buffer[(nextPosition + buffer.length - 1) % buffer.length];
  }

  public void resetHeadValue(long value) {
    if (isEmpty()) {
      throw new IllegalArgumentException("circle buffer is empty, position=" + nextPosition);
    }

    buffer[(nextPosition + buffer.length - 1) % buffer.length] = value;
  }
}
