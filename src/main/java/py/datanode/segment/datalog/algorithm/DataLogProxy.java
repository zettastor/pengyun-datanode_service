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
