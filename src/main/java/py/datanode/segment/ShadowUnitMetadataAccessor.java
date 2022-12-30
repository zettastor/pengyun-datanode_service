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
import py.common.tlsf.bytebuffer.manager.TlsfByteBufferManager;
import py.common.tlsf.bytebuffer.manager.TlsfByteBufferManagerFactory;

public class ShadowUnitMetadataAccessor {
  private static final Logger logger = LoggerFactory.getLogger(ShadowUnitMetadataAccessor.class);

  public static void writeShadowUnitMetadataToDisk(ShadowUnitMetadata unit) throws Exception {
    logger.warn("write segment unit metadata to disk: {}", unit);

    ObjectMapper mapper = new ObjectMapper();
    byte[] metadataBytes = mapper.writeValueAsBytes(unit);
    byte[] bitmapBytes = unit.getBitmap().toByteArray();
    logger.debug("byte length: {} parsed results: {}", metadataBytes.length,
        new String(metadataBytes));
    Validate.isTrue(metadataBytes.length < ArchiveOptions.SEGMENTUNIT_METADATA_LENGTH);

    TlsfByteBufferManager tlsfByteBufferManager = TlsfByteBufferManagerFactory.instance();
    Validate.notNull(tlsfByteBufferManager);

    ByteBuffer buf = tlsfByteBufferManager.blockingAllocate(
        ArchiveOptions.SEGMENTUNIT_METADATA_LENGTH + ArchiveOptions.SEGMENTUNIT_BITMAP_LENGTH);

    try {
      buf.putLong(ArchiveOptions.SHADOW_UNIT_MAGIC).put(metadataBytes);
      buf.position(ArchiveOptions.SEGMENTUNIT_METADATA_LENGTH);
      buf.put(bitmapBytes);
      buf.clear();
      unit.getStorage().write(unit.getMetadataOffsetInArchive(), buf);
    } finally {
      tlsfByteBufferManager.release(buf);
    }
  }

  /**
   * read shadow unit from the disk.
   */
  public static ShadowUnitMetadata readShadowMetadataFromBuffer(ByteBuffer buffer)
      throws IOException {
    long magicNumber = buffer.getLong();
    Validate.isTrue(magicNumber == ArchiveOptions.SHADOW_UNIT_MAGIC);

    byte[] bytes = new byte[ArchiveOptions.SEGMENTUNIT_METADATA_LENGTH - Long.SIZE];
    buffer.get(bytes);
    ObjectMapper mapper = new ObjectMapper();
    ShadowUnitMetadata metadata = mapper
        .readValue(bytes, 0, ArchiveOptions.SEGMENTUNIT_METADATA_LENGTH, ShadowUnitMetadata.class);
    return metadata;
  }

}
