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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.datanode.segment.datalog.MutationLogEntry;
import py.datanode.segment.datalog.MutationLogEntryReader;
import py.datanode.segment.datalog.MutationLogEntryWriter;
import py.datanode.segment.datalog.MutationLogReaderWriterFactory;
import py.third.rocksdb.RocksDbKvSerializer;

public class LogStorageRocksDbIndexValue implements RocksDbKvSerializer {
  private static final Logger logger = LoggerFactory.getLogger(LogStorageRocksDbIndexValue.class);
  private MutationLogEntry log;
  private MutationLogReaderWriterFactory factory;

  private MutationLogEntryWriter writer;
  private MutationLogEntryReader reader;

  public LogStorageRocksDbIndexValue(MutationLogEntry log, MutationLogReaderWriterFactory factory,
      MutationLogEntryWriter writer, MutationLogEntryReader reader) {
    this.log = log;
    this.factory = factory;
    this.writer = writer;
    this.reader = reader;
  }

  public MutationLogEntry getLog() {
    return log;
  }

  @Override
  public int size() {
    return factory.getSerializedLogSize();
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

    ByteArrayOutputStream outputStream = new ByteArrayOutputStream(size());
    synchronized (writer) {
      try {
        writer.close();
        writer.open(outputStream);
        writer.write(log);
      } catch (IOException ex) {
        logger.error("log writer entry write log failed", ex);
        throw new IllegalArgumentException("log writer entry write log failed");
      } finally {
        try {
          writer.close();
        } catch (Exception ex) {
          logger.error("close log writer failed", ex);
          throw ex;
        }
      }
    }

    buffer.put(outputStream.toByteArray());
  }

  @Override
  public boolean deserialize(byte[] bytes) throws IOException {
    ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes);
    synchronized (reader) {
      try {
        reader.open(inputStream);
        this.log = reader.read();

      } catch (IOException ex) {
        logger.error("log writer entry write log failed", ex);
        return false;
      } finally {
        try {
          reader.close();
        } catch (Exception ex) {
          logger.error("close log reader failed", ex);
        }
      }
    }
    return true;
  }

  @Override
  public boolean deserialize(ByteBuffer buffer) throws IOException {
    ByteArrayInputStream inputStream = new ByteArrayInputStream(buffer.array());
    synchronized (reader) {
      try {
        reader.open(inputStream);
        this.log = reader.read();
      } catch (IOException ex) {
        logger.error("log writer entry write log failed", ex);
        return false;
      } finally {
        try {
          reader.close();
        } catch (Exception ex) {
          logger.error("close log reader failed", ex);
        }
      }
    }

    return true;
  }

  public int compare(RocksDbKvSerializer another) throws IOException {
    throw new IOException("invalid compare called!");
  }
}
