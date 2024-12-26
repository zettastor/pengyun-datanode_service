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

package py.datanode.segment.datalog;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.Validate;
import py.archive.segment.SegmentVersion;

public class LogIdGenerator {
  static final int EPOCH_OFFSET = 43;
  static final int TP1_OFFSET = 42;
  static final int TP2_OFFSET = 41;

  private AtomicLong curLogId;

  public LogIdGenerator(SegmentVersion version) {
    this(version.getEpoch());
  }

  public LogIdGenerator(int epoch, boolean forTempPrimary) {
    this(epoch, forTempPrimary, false);
  }

  public LogIdGenerator(int epoch, boolean forTempPrimary, boolean newTempPrimary) {
    Validate.isTrue(epoch >= 0);

    long base = epoch & 0xfffffL;

    long logId = (base << EPOCH_OFFSET) + (System.currentTimeMillis() & 0xffffffffL);
    if (forTempPrimary) {
      logId += 1L << TP1_OFFSET;
      if (newTempPrimary) {
        logId += 1L << TP2_OFFSET;
      }
    }
    curLogId = new AtomicLong(logId);
  }

  public LogIdGenerator(int epoch) {
    this(epoch, false);
  }

  public long newId() {
    return curLogId.getAndIncrement();
  }

  public long currentLogId() {
    return curLogId.get();
  }

  @Override
  public String toString() {
    return "LogIdGenerator{" 
        + "curLogId=" + curLogId.get() 
        + '}';
  }
}
