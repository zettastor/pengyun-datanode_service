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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.AbstractCollection;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Queue;

public class CircularFifoQueue<E> extends AbstractCollection<E> implements Queue<E>, Serializable {
  /**
   * Serialization version.
   */
  private static final long serialVersionUID = -8423413834657610406L;
  /**
   * Capacity of the queue.
   */
  private final int maxElements;
  /**
   * Underlying storage array.
   */
  private transient E[] elements;
  /**
   * Array index of first (oldest) queue element.
   */
  private transient int start = 0;

  private transient int end = 0;
  /**
   * Flag to indicate if the queue is currently full.
   */
  private transient boolean full = false;

  @SuppressWarnings("unchecked")
  public CircularFifoQueue(final int size) {
    if (size <= 0) {
      throw new IllegalArgumentException("The size must be greater than 0");
    }
    elements = (E[]) new Object[size];
    maxElements = elements.length;
  }

  public CircularFifoQueue(final Collection<? extends E> coll) {
    this(coll.size());
    addAll(coll);
  }

  private void writeObject(final ObjectOutputStream out) throws IOException {
    out.defaultWriteObject();
    out.writeInt(size());
    for (final E e : this) {
      out.writeObject(e);
    }
  }

  @SuppressWarnings("unchecked")
  private void readObject(final ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
    elements = (E[]) new Object[maxElements];
    final int size = in.readInt();
    for (int i = 0; i < size; i++) {
      elements[i] = (E) in.readObject();
    }
    start = 0;
    full = size == maxElements;
    if (full) {
      end = 0;
    } else {
      end = size;
    }
  }

  @Override
  public int size() {
    int size = 0;

    if (end < start) {
      size = maxElements - start + end;
    } else if (end == start) {
      size = full ? maxElements : 0;
    } else {
      size = end - start;
    }

    return size;
  }

  /**
   * Returns true if this queue is empty; false otherwise.
   *
   * @return true if this queue is empty
   */
  @Override
  public boolean isEmpty() {
    return size() == 0;
  }

  public boolean isFull() {
    return false;
  }

  private boolean isAtFullCapacity() {
    return size() == maxElements;
  }

  public int maxSize() {
    return maxElements;
  }

  /**
   * Clears this queue.
   */
  @Override
  public void clear() {
    full = false;
    start = 0;
    end = 0;
    Arrays.fill(elements, null);
  }

  @Override
  public boolean add(final E element) {
    if (null == element) {
      throw new NullPointerException("Attempted to add null object to queue");
    }

    if (isAtFullCapacity()) {
      remove();
    }

    elements[end++] = element;

    if (end >= maxElements) {
      end = 0;
    }

    if (end == start) {
      full = true;
    }

    return true;
  }

  public E get(final int index) {
    final int sz = size();
    if (index < 0 || index >= sz) {
      throw new NoSuchElementException(
          String.format("The specified index (%1$d) is outside the available range [0, %2$d)",
              Integer.valueOf(index), Integer.valueOf(sz)));
    }

    final int idx = (start + index) % maxElements;
    return elements[idx];
  }

  public boolean offer(E element) {
    return add(element);
  }

  public E poll() {
    if (isEmpty()) {
      return null;
    }
    return remove();
  }

  @Override
  public E element() {
    if (isEmpty()) {
      throw new NoSuchElementException("queue is empty");
    }
    return peek();
  }
  

  public E peek() {
    if (isEmpty()) {
      return null;
    }
    return elements[start];
  }
  

  public E peekTail() {
    if (isEmpty()) {
      return null;
    }
    return elements[decrement(end)];
  }
  

  public E remove() {
    if (isEmpty()) {
      throw new NoSuchElementException("queue is empty");
    }

    final E element = elements[start];
    if (null != element) {
      elements[start++] = null;

      if (start >= maxElements) {
        start = 0;
      }
      full = false;
    }
    return element;
  }

  private int increment(int index) {
    index++;
    if (index >= maxElements) {
      index = 0;
    }
    return index;
  }

  private int decrement(int index) {
    index--;
    if (index < 0) {
      index = maxElements - 1;
    }
    return index;
  }

  @Override
  public Iterator<E> iterator() {
    return new Iterator<E>() {
      private int index = start;
      private int lastReturnedIndex = -1;
      private boolean isFirst = full;

      public boolean hasNext() {
        return isFirst || index != end;
      }

      public E next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        isFirst = false;
        lastReturnedIndex = index;
        index = increment(index);
        return elements[lastReturnedIndex];
      }

      public void remove() {
        if (lastReturnedIndex == -1) {
          throw new IllegalStateException();
        }

        if (lastReturnedIndex == start) {
          CircularFifoQueue.this.remove();
          lastReturnedIndex = -1;
          return;
        }

        int pos = lastReturnedIndex + 1;
        if (start < lastReturnedIndex && pos < end) {
          System.arraycopy(elements, pos, elements, lastReturnedIndex, end - pos);
        } else {
          while (pos != end) {
            if (pos >= maxElements) {
              elements[pos - 1] = elements[0];
              pos = 0;
            } else {
              elements[decrement(pos)] = elements[pos];
              pos = increment(pos);
            }
          }
        }

        lastReturnedIndex = -1;
        end = decrement(end);
        elements[end] = null;
        full = false;
        index = decrement(index);
      }

    };
  }
}
