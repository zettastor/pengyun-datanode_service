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

package py.datanode.segment;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.ArchiveOptions;
import py.archive.brick.BrickMetadata;
import py.archive.segment.SegmentUnitMetadata;
import py.common.tlsf.bytebuffer.manager.TlsfByteBufferManager;
import py.common.tlsf.bytebuffer.manager.TlsfByteBufferManagerFactory;
import py.exception.StorageException;
import py.storage.Storage;

public class SegmentUnitMetadataAccessor {
  private static final Logger logger = LoggerFactory.getLogger(SegmentUnitMetadataAccessor.class);

  /**
   * Write the new meta data to the corresponding disk.
   */
  public static void writeSegmentUnitMetaToDisk(SegmentUnitMetadata newMetadata) throws Exception {
    logger.info("write segment unit metadata to disk: {}", newMetadata);

    ObjectMapper mapper = new ObjectMapper();
    byte[] bytes = mapper.writeValueAsBytes(newMetadata);
    logger.debug("byte length: {} parsed results: {}", bytes.length, new String(bytes));
    Validate
        .isTrue(bytes.length + Long.SIZE / Byte.SIZE < ArchiveOptions.SEGMENTUNIT_METADATA_LENGTH,
            "byte 's length is %d", bytes.length);

    TlsfByteBufferManager tlsfByteBufferManager = TlsfByteBufferManagerFactory.instance();
    Validate.notNull(tlsfByteBufferManager);

    ByteBuffer buf = tlsfByteBufferManager
        .blockingAllocate(ArchiveOptions.SEGMENTUNIT_METADATA_LENGTH);

    try {
      buf.putLong(ArchiveOptions.SEGMENT_UNIT_MAGIC).put(bytes).clear();
      newMetadata.getStorage().write(newMetadata.getMetadataOffsetInArchive(), buf);
    } finally {
      tlsfByteBufferManager.release(buf);
    }
  }

  /**
   * read segment unit from the disk.
   */
  public static SegmentUnitMetadata readSegmentUnitMetadataFromBuffer(ByteBuffer buffer)
      throws IOException {
    long magicNumber = buffer.getLong();
    Validate.isTrue(magicNumber == ArchiveOptions.SEGMENT_UNIT_MAGIC);

    int bytesLen = ArchiveOptions.SEGMENTUNIT_METADATA_LENGTH - Long.SIZE / Byte.SIZE;
    byte[] bytes = new byte[bytesLen];
    buffer.get(bytes);
    logger.debug("read value string {}", new String(bytes));
    ObjectMapper mapper = new ObjectMapper();
    SegmentUnitMetadata metadata = mapper.readValue(bytes, 0, bytesLen, SegmentUnitMetadata.class);
    return metadata;
  }

  /**
   * read brick from the disk.
   */
  public static BrickMetadata readBrickMetadataFromBuffer(ByteBuffer buffer) throws IOException {
    long magicNumber = buffer.getLong();
    Validate.isTrue(magicNumber == ArchiveOptions.BRICK_MAGIC);

    int bytesLen = ArchiveOptions.BRICK_METADATA_LENGTH - Long.SIZE / Byte.SIZE;
    byte[] bytes = new byte[bytesLen];
    buffer.get(bytes);
    logger.debug("read value string {}", new String(bytes));
    ObjectMapper mapper = new ObjectMapper();
    BrickMetadata metadata = mapper.readValue(bytes, 0, bytesLen, BrickMetadata.class);
    return metadata;
  }

  public static void cleanAllSegmentMetadata(int maxSegUnitCount, Storage storage)
      throws StorageException {
    if (maxSegUnitCount <= 0) {
      logger.warn("the storage's size is less than one segment unit");
      return;
    }

    TlsfByteBufferManager tlsfByteBufferManager = TlsfByteBufferManagerFactory.instance();
    Validate.notNull(tlsfByteBufferManager);

    ByteBuffer buffer = tlsfByteBufferManager.blockingAllocate(1 * 1024 * 1024);
    buffer.clear();

    try {
      for (int i = 0; i < buffer.capacity(); i++) {
        buffer.put((byte) 0);
      }

      long pos = ArchiveOptions.SEGMENTUNIT_DESCDATA_REGION_OFFSET;
      long leftSize = ArchiveOptions.ALL_FLEXIBLE_LENGTH
          + ArchiveOptions.BRICK_DESCDATA_LENGTH * maxSegUnitCount;
      logger.warn("the pos is {},the size is {}", pos, leftSize);

      while (leftSize > 0) {
        buffer.clear();
        long sizeToWrite = Math.min(buffer.remaining(), leftSize);
        storage.write(pos, buffer);
        pos += sizeToWrite;
        leftSize -= sizeToWrite;
      }

    } finally {
      tlsfByteBufferManager.release(buffer);
    }

    logger.debug("clean the segment unit meta data area succesfully");

  }
}
