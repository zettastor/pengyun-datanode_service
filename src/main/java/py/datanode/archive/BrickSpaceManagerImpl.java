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

package py.datanode.archive;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.ArchiveOptions;
import py.archive.brick.BrickMetadata;
import py.archive.brick.BrickSpaceManager;
import py.common.bitmap.Bitmap;

public class BrickSpaceManagerImpl implements BrickSpaceManager {
  private static final Logger logger = LoggerFactory.getLogger(BrickSpaceManagerImpl.class);
  private final BrickMetadata brickMetadata;
  private final Bitmap allocatedBitmap;
  private final AtomicBoolean bitmapHasFormat = new AtomicBoolean(false);
  private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();

  private final Map<Integer, TreeSet<Long>> freePagesMapOnSequential = new HashMap<>();
  private int freePageCount = 0;

  public BrickSpaceManagerImpl(BrickMetadata brickMetadata) {
    this.brickMetadata = brickMetadata;
    this.allocatedBitmap = brickMetadata.getAllocateBitmap();
  }

  public int getFreeSpace() {
    readWriteLock.readLock().lock();
    try {
      return this.freePageCount;
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  public void markAllPageUsed() {
    logger.warn("mark all brick pages used {}", brickMetadata);
    readWriteLock.writeLock().lock();
    try {
      freePagesMapOnSequential.clear();
      allocatedBitmap.clear();
      allocatedBitmap.inverse();
      bitmapHasFormat.set(true);

      this.freePageCount = 0;
    } finally {
      brickMetadata.setFreePageCount(this.freePageCount);
      readWriteLock.writeLock().unlock();
    }
  }

  public boolean isPageFree(int index) {
    readWriteLock.readLock().lock();
    try {
      return !allocatedBitmap.get(index);
    } finally {
      readWriteLock.readLock().unlock();
    }
  }

  private int pageIndexByOffset(long offset) {
    return (int) ((offset - brickMetadata.getDataOffset()) / ArchiveOptions.PAGE_PHYSICAL_SIZE);
  }

  private void setAllocated(long offset, int count) {
    int pageIndex = pageIndexByOffset(offset);

    Validate.isTrue(pageIndex >= 0);
    Validate.isTrue(pageIndex < brickMetadata.getPageCount());

    while (count-- > 0) {
      allocatedBitmap.set(pageIndex++);
    }
  }

  private void clearAllocated(long offset, int count) {
    int pageIndex = pageIndexByOffset(offset);

    Validate.isTrue(pageIndex >= 0);
    Validate.isTrue(pageIndex < brickMetadata.getPageCount());

    while (count-- > 0) {
      allocatedBitmap.clear(pageIndex++);
    }
  }

}
