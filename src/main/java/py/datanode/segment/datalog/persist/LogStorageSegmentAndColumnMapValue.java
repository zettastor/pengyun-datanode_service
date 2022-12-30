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
