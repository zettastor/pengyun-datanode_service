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
import java.util.concurrent.CompletableFuture;

public class DataLogsApplierWithByteBuf extends DataLogsApplier {
  private ByteBuf destination;
  private int initWriteIndex;

  public DataLogsApplierWithByteBuf(long destinationPos, int destinationLength, int pageSize) {
    super(destinationPos, destinationLength, pageSize);
  }

  @Override
  protected void applyLogData(DataLog log, int offsetInDestination, int offsetInLog,
      int length) {
    if (destination == null) {
      throw new NullPointerException("no destination set");
    }
    destination.writerIndex(initWriteIndex + offsetInDestination);
    log.getData(destination, offsetInLog, length);
  }

  @Override
  protected CompletableFuture<Void> loadPageData(boolean wholePageCovered) {
    return CompletableFuture.completedFuture(null);
  }

  public void setDestination(ByteBuf destination) {
    if (this.destination != null) {
      throw new IllegalArgumentException("destination already set");
    }
    this.destination = destination;
    this.initWriteIndex = destination.writerIndex();
  }

}
