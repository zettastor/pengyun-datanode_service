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
import py.third.rocksdb.RocksDbKvSerializer;

public class LogStorageSegmentAndColumnMapValue implements RocksDbKvSerializer {
  private static final int ARCHIVE_ID_SIZE = Long.SIZE / Byte.SIZE;
  private static final int SIZE = ARCHIVE_ID_SIZE;

  private long archiveId;

  public LogStorageSegmentAndColumnMapValue(long archiveId) {
    this.archiveId = archiveId;
  }

  @Override
  public int size() {
    return SIZE;
  }

  public long getArchiveId() {
    return archiveId;
  }

  public void setArchiveId(long archiveId) {
    this.archiveId = archiveId;
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

    buffer.putLong(archiveId);
  }

  @Override
  public boolean deserialize(byte[] bytes) throws IOException {
    ByteBuffer buffer = ByteBuffer.wrap(bytes);

    return deserialize(buffer);
  }

  @Override
  public boolean deserialize(ByteBuffer buffer) throws IOException {
    this.archiveId = buffer.getLong();

    return true;
  }
  

  public int compare(RocksDbKvSerializer another) throws IOException {
    if (!(another instanceof LogStorageSegmentAndColumnMapKey)) {
      throw new IOException("invalid compare called!");
    }

    LogStorageSegmentAndColumnMapValue key = (LogStorageSegmentAndColumnMapValue) another;

    if (this.archiveId > key.archiveId) {
      return 1;
    } else if (this.archiveId < key.archiveId) {
      return -1;
    }

    return 0;
  }
}
