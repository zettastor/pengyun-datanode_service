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

public abstract class ImmutableStatusDataLog extends BasicLog {
  private final LogStatus logStatus;
  private final boolean isApplied;
  private final boolean isPersisted;

  public ImmutableStatusDataLog(long logId, long offset, int length, long logUuid,
       LogStatus logStatus, boolean isApplied, boolean isPersisted) {
    super(logId, offset, length, logUuid);
    this.logStatus = logStatus;
    this.isApplied = isApplied;
    this.isPersisted = isPersisted;
  }

  @Override
  public LogStatus getLogStatus() {
    return logStatus;
  }

  @Override
  public void setLogStatus(LogStatus logStatus) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isApplied() {
    return isApplied;
  }

  @Override
  public boolean setApplied() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isPersisted() {
    return isPersisted;
  }

  @Override
  public boolean setPersisted() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void getData(ByteBuffer destination, int offset, int length) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void getData(ByteBuf destination, int offset, int length) {
    throw new UnsupportedOperationException();
  }

  @Override
  public byte[] getData() {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getCheckSum() {
    throw new UnsupportedOperationException();
  }

}
