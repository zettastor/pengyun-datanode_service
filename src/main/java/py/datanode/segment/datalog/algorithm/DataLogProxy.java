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

package py.datanode.segment.datalog.algorithm;

import io.netty.buffer.ByteBuf;
import java.nio.ByteBuffer;
import py.datanode.segment.datalog.MutationLogEntry.LogStatus;

public class DataLogProxy implements DataLog {
  private final DataLog delegate;

  public DataLogProxy(DataLog delegate) {
    this.delegate = delegate;
  }

  @Override
  public long getLogId() {
    return delegate.getLogId();
  }

  @Override
  public long getOffset() {
    return delegate.getOffset();
  }

  @Override
  public int getLength() {
    return delegate.getLength();
  }

  @Override
  public LogStatus getLogStatus() {
    return delegate.getLogStatus();
  }

  @Override
  public void setLogStatus(LogStatus logStatus) {
    delegate.setLogStatus(logStatus);
  }

  @Override
  public boolean isApplied() {
    return delegate.isApplied();
  }

  @Override
  public boolean setApplied() {
    return delegate.setApplied();
  }

  @Override
  public boolean isPersisted() {
    return delegate.isPersisted();
  }

  @Override
  public boolean setPersisted() {
    return delegate.setPersisted();
  }

  @Override
  public void getData(ByteBuffer destination, int offset, int length) {
    delegate.getData(destination, offset, length);
  }

  @Override
  public void getData(ByteBuf destination, int offset, int length) {
    delegate.getData(destination, offset, length);
  }

  @Override
  public byte[] getData() {
    return delegate.getData();
  }

  @Override
  public long getLogUuid() {
    return delegate.getLogUuid();
  }

  @Override
  public long getCheckSum() {
    return delegate.getCheckSum();
  }

}
