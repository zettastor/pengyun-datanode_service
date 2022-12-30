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
import py.archive.segment.SegId;
import py.third.rocksdb.RocksDbKvSerializer;

public class LogStorageRocksDbIndexKey implements RocksDbKvSerializer {
  private static final int LOG_ID_SIZE = Long.SIZE / Byte.SIZE;
  private static final int VOLUME_ID_SIZE = Long.SIZE / Byte.SIZE;
  private static final int INDEX_ID_SIZE = Integer.SIZE / Byte.SIZE;
  private static final int SIZE = LOG_ID_SIZE + VOLUME_ID_SIZE + INDEX_ID_SIZE;

  private long logId;
  private long volumeId;
  private int indexId;

  public LogStorageRocksDbIndexKey(long id, SegId segId) {
    this.logId = id;
    this.volumeId = segId.getVolumeId().getId();
    this.indexId = segId.getIndex();
  }

  public long getLogId() {
    return logId;
  }

  public void setLogId(long logId) {
    this.logId = logId;
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
    buffer.putLong(logId);
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
    this.logId = buffer.getLong();

    return true;
  }

  public int compare(RocksDbKvSerializer another) throws IOException {
    if (!(another instanceof LogStorageRocksDbIndexKey)) {
      throw new IOException("invalid compare called!");
    }

    LogStorageRocksDbIndexKey key = (LogStorageRocksDbIndexKey) another;
    if (this.logId > key.logId) {
      return 1;
    } else if (this.logId < key.logId) {
      return -1;
    }

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
