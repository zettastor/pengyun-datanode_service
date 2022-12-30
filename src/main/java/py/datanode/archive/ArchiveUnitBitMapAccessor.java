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

import java.nio.ByteBuffer;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.AbstractSegmentUnitMetadata;
import py.archive.ArchiveOptions;
import py.archive.brick.BrickMetadata;
import py.archive.segment.SegmentUnitBitmap;
import py.common.bitmap.Bitmap;
import py.common.tlsf.bytebuffer.manager.TlsfByteBufferManager;
import py.common.tlsf.bytebuffer.manager.TlsfByteBufferManagerFactory;
import py.exception.StorageException;

public class ArchiveUnitBitMapAccessor {
  private static final Logger logger = LoggerFactory.getLogger(ArchiveUnitBitMapAccessor.class);

  public static void writeBitMapToDisk(AbstractSegmentUnitMetadata unit) throws StorageException {
    logger.debug("persisting bit map to disk for {}", unit.getSegId());
    byte[] bytes = unit.getBitmap().toByteArray();

    TlsfByteBufferManager tlsfByteBufferManager = TlsfByteBufferManagerFactory.instance();
    Validate.notNull(tlsfByteBufferManager);

    ByteBuffer buf = tlsfByteBufferManager
        .blockingAllocate(ArchiveOptions.SEGMENTUNIT_BITMAP_LENGTH);

    try {
      buf.put(bytes).clear();
      unit.getStorage()
          .write(unit.getMetadataOffsetInArchive() + ArchiveOptions.SEGMENTUNIT_METADATA_LENGTH,
              buf);
    } finally {
      tlsfByteBufferManager.release(buf);
    }
  }

  public static SegmentUnitBitmap readBitMapFromDisk(AbstractSegmentUnitMetadata unit)
      throws StorageException {
    TlsfByteBufferManager tlsfByteBufferManager = TlsfByteBufferManagerFactory.instance();
    Validate.notNull(tlsfByteBufferManager);

    ByteBuffer buf = tlsfByteBufferManager
        .blockingAllocate(ArchiveOptions.SEGMENTUNIT_BITMAP_LENGTH);

    logger.debug("buf 's capacity {}, segment unit bitmap length {}", buf.capacity(),
        ArchiveOptions.SEGMENTUNIT_BITMAP_LENGTH);

    try {
      unit.getStorage()
          .read(unit.getMetadataOffsetInArchive() + ArchiveOptions.SEGMENTUNIT_METADATA_LENGTH,
              buf);
      buf.clear();

      int byteCountOfBitmap = SegmentUnitBitmap
          .bitMapLength(ArchiveOptions.PAGE_NUMBER_PER_SEGMENT);
      byte[] bytesArray = new byte[byteCountOfBitmap];

      buf.get(bytesArray, 0, byteCountOfBitmap);
      return SegmentUnitBitmap.valueOf(bytesArray);
    } finally {
      tlsfByteBufferManager.release(buf);
    }
  }

  public static Bitmap readBrickAllocatedBitMapFromDisk(BrickMetadata brickMetadata)
      throws StorageException {
    TlsfByteBufferManager tlsfByteBufferManager = TlsfByteBufferManagerFactory.instance();
    Validate.notNull(tlsfByteBufferManager);

    int byteCountOfBitmap = ArchiveOptions.BRICK_BITMAP_LENGTH;
    ByteBuffer buf = tlsfByteBufferManager.blockingAllocate(byteCountOfBitmap);

    logger.debug("buf 's capacity {}, segment unit bitmap length {}", buf.capacity(),
        ArchiveOptions.BRICK_BITMAP_LENGTH);

    try {
      brickMetadata.getStorage()
          .read(brickMetadata.getMetadataOffsetInArchive() + ArchiveOptions.BRICK_METADATA_LENGTH,
              buf);
      buf.clear();

      byte[] bytesArray = new byte[byteCountOfBitmap];

      buf.get(bytesArray, 0, byteCountOfBitmap);
      return BrickMetadata.bitmapValueOf(bytesArray);
    } finally {
      tlsfByteBufferManager.release(buf);
    }
  }

  public static void writeBrickBitMapToDisk(BrickMetadata brickMetadata) throws StorageException {
    logger.debug("persisting bit map to disk for {}", brickMetadata.getSegId());
    byte[] bytes = brickMetadata.bitmapByteArray();

    int byteCountOfBitmap = ArchiveOptions.BRICK_BITMAP_LENGTH;
    TlsfByteBufferManager tlsfByteBufferManager = TlsfByteBufferManagerFactory.instance();
    Validate.notNull(tlsfByteBufferManager);

    ByteBuffer buf = tlsfByteBufferManager.blockingAllocate(byteCountOfBitmap);

    try {
      buf.put(bytes).clear();
      brickMetadata.getStorage()
          .write(brickMetadata.getMetadataOffsetInArchive() + ArchiveOptions.BRICK_METADATA_LENGTH,
              buf);
    } finally {
      tlsfByteBufferManager.release(buf);
    }
  }
}
