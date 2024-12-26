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

package py.datanode.segment.datalog.persist;

import java.io.IOException;
import java.nio.ByteBuffer;
import py.archive.segment.SegId;
import py.third.rocksdb.RocksDbKvSerializer;

public class LogStorageSegmentAndColumnMapKey implements RocksDbKvSerializer {
  private static final int VOLUME_ID_SIZE = Long.SIZE / Byte.SIZE;
  private static final int INDEX_ID_SIZE = Integer.SIZE / Byte.SIZE;
  private static final int SIZE = VOLUME_ID_SIZE + INDEX_ID_SIZE;

  private long volumeId;
  private int indexId;

  public LogStorageSegmentAndColumnMapKey(SegId segId) {
    this.volumeId = segId.getVolumeId().getId();
    this.indexId = segId.getIndex();
  }

  public long getVolumeId() {
    return volumeId;
  }

  public void setVolumeId(long volumeId) {
    this.volumeId = volumeId;
  }

  public int getIndexId() {
    return indexId;
  }

  public void setIndexId(int indexId) {
    this.indexId = indexId;
  }

  @Override
  public int size() {
    return SIZE;
  }

  @Override
  public void serialize(byte[] bytes) throws IOException {
    if (bytes.length < size()) {
      throw new IllegalArgumentException("container of bytes is too small; size: " + size());
    }

    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    serialize(buffer);
  }

  @Override
  public void serialize(ByteBuffer buffer) throws IOException {
    if (buffer.limit() < size()) {
      throw new IllegalArgumentException("container of bytes is too small; size: " + size());
    }

    buffer.putLong(volumeId);
    buffer.putInt(indexId);
  }

  @Override
  public boolean deserialize(byte[] bytes) throws IOException {
    ByteBuffer buffer = ByteBuffer.wrap(bytes);

    return deserialize(buffer);
  }

  @Override
  public boolean deserialize(ByteBuffer buffer) throws IOException {
    this.volumeId = buffer.getLong();
    this.indexId = buffer.getInt();

    return true;
  }

  public int compare(RocksDbKvSerializer another) throws IOException {
    if (!(another instanceof LogStorageSegmentAndColumnMapKey)) {
      throw new IOException("invalid compare called!");
    }

    LogStorageSegmentAndColumnMapKey key = (LogStorageSegmentAndColumnMapKey) another;

    if (this.volumeId > key.volumeId) {
      return 1;
    } else if (this.volumeId < key.volumeId) {
      return -1;
    }

    if (this.indexId > key.indexId) {
      return 1;
    } else if (this.indexId < key.indexId) {
      return -1;
    }

    return 0;
  }
}
