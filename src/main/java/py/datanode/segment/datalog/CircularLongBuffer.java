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
