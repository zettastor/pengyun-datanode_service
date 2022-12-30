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

package py.datanode.segment.copy.bitmap;

import java.util.BitSet;

public class CopyPageBitmapImpl implements CopyPageBitmap {
  private BitSet bitSet;
  private int size;

  public CopyPageBitmapImpl(byte[] bitmap, int pageCount) {
    this.bitSet = BitSet.valueOf(bitmap);
    this.size = pageCount;
  }

  public CopyPageBitmapImpl(int pageCount) {
    this.bitSet = new BitSet(pageCount);
    this.size = pageCount;
  }

  static void subBitmapRangeCheck(int fromIndex, int toIndex, int size) {
    if (fromIndex < 0) {
      throw new IndexOutOfBoundsException("fromIndex = " + fromIndex);
    }
    if (toIndex > size) {
      throw new IndexOutOfBoundsException("toIndex = " + toIndex);
    }
    if (fromIndex > toIndex) {
      throw new IllegalArgumentException("fromIndex(" + fromIndex 
          + ") > toIndex(" + toIndex + ")");
    }
  }

  private void rangeCheck(int index) {
    if (index >= size()) {
      throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + size());
    }
  }

  @Override
  public synchronized byte[] array() {
    return this.bitSet.toByteArray();
  }

  @Override
  public synchronized boolean get(int pageIndex) {
    rangeCheck(pageIndex);
    return bitSet.get(pageIndex);
  }

  @Override
  public synchronized void clear(int pageIndex) {
    rangeCheck(pageIndex);
    bitSet.clear(pageIndex);
  }

  @Override
  public synchronized void set(int pageIndex) {
    rangeCheck(pageIndex);
    bitSet.set(pageIndex);
  }

  @Override
  public synchronized void set(int fromIndex, int toIndex) {
    if (fromIndex == toIndex) {
      return;
    }
    rangeCheck(fromIndex);
    rangeCheck(toIndex - 1);
    bitSet.set(fromIndex, toIndex);
  }

  @Override
  public boolean isFull() {
    int nextClear = bitSet.nextClearBit(0);
    return nextClear == -1 || nextClear >= size();
  }

  @Override
  public boolean isEmpty() {
    return bitSet.isEmpty();
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public int offset() {
    return 0;
  }

  @Override
  public int cardinality() {
    return bitSet.cardinality();
  }

  @Override
  public CopyPageBitmap subBitmap(int fromIndex, int toIndex) {
    subBitmapRangeCheck(fromIndex, toIndex, size());
    return new SubBitmap(this, fromIndex, toIndex);
  }

  @Override
  public String toString() {
    return "CopyPageBitmapImpl [size=" + size() + ", done=" + cardinality() + "]";
  }

  private class SubBitmap implements CopyPageBitmap {
    int fromIndex;
    int toIndex;
    CopyPageBitmap parent;

    public SubBitmap(CopyPageBitmap parent, int fromIndex, int toIndex) {
      this.parent = parent;
      this.fromIndex = fromIndex;
      this.toIndex = toIndex;
    }

    @Override
    public int offset() {
      return fromIndex + parent.offset();
    }

    @Override
    public boolean get(int pageIndex) {
      return parent.get(pageIndex + fromIndex);
    }

    @Override
    public void set(int pageIndex) {
      parent.set(pageIndex + fromIndex);
    }

    @Override
    public void set(int fromIndex, int toIndex) {
      parent.set(fromIndex + this.fromIndex, toIndex + this.fromIndex);
    }

    @Override
    public boolean isFull() {
      int offset = offset();
      int nextClear = bitSet.nextClearBit(offset);
      return nextClear == -1 || nextClear >= offset + size();
    }

    @Override
    public boolean isEmpty() {
      int offset = offset();
      int nextSet = bitSet.nextSetBit(offset);
      return nextSet == -1 || nextSet >= offset + size();
    }

    @Override
    public void clear(int pageIndex) {
      parent.clear(pageIndex + fromIndex);
    }

    @Override
    public byte[] array() {
      return bitSet.get(fromIndex, toIndex).toByteArray();
    }

    @Override
    public int size() {
      return toIndex - fromIndex;
    }

    @Override
    public int cardinality() {
      return bitSet.get(fromIndex, toIndex).cardinality();
    }

    @Override
    public CopyPageBitmap subBitmap(int fromIndex, int toIndex) {
      return new SubBitmap(this, fromIndex, toIndex);
    }

    @Override
    public String toString() {
      return "SubBitmap[offset=" + offset() + ", size=" + size() + ", done=" + cardinality() + "]";
    }
  }
}
