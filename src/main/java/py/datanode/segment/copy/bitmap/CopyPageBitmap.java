

package py.datanode.segment.copy.bitmap;

import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public interface CopyPageBitmap {
  boolean get(int pageIndex);

  void set(int pageIndex);

  void set(int fromIndex, int toIndex);

  boolean isFull();

  boolean isEmpty();

  void clear(int pageIndex);

  byte[] array();

  int size();

  int offset();

  int cardinality();

  CopyPageBitmap subBitmap(int fromIndex, int toIndex);
}
