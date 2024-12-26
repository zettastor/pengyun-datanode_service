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
