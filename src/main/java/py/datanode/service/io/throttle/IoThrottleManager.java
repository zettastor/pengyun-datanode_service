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

package py.datanode.service.io.throttle;

import java.util.Collection;
import py.archive.segment.SegId;
import py.common.RequestIdBuilder;
import py.common.struct.Pair;
import py.datanode.archive.RawArchive;

public interface IoThrottleManager {
  Pair<Boolean, Integer> register(RawArchive archive, SegId segId, int countOfPageToCopy,
      long sessionId);

  void unregister(RawArchive archive, SegId segId);

  int throttle(SegId segId, int copyCount);

  boolean exist(SegId segId);

  void finish(RawArchive archive, SegId segId);

  void addTotal(SegId segId, int count);

  boolean addAlready(SegId segId, int count);

  void markNormalIoComes(RawArchive archive, boolean isWrite);

  Collection<CopyPageSampleInfo> getCopyPageSampleInfos();

  enum IoType {
    Clone,
    Rebalance,
    CopyPage,
    SnapshotGC;

    static IoType getIoType(long sessionId) {
      return sessionId == 0 ? Clone : sessionId < 0 ? Rebalance : CopyPage;
    }

    public long newSessionId() {
      if (this == CopyPage) {
        return Math.abs(RequestIdBuilder.get());
      } else if (this == Rebalance) {
        return 0 - Math.abs(RequestIdBuilder.get());
      } else {
        return 0;
      }
    }
  }
}
