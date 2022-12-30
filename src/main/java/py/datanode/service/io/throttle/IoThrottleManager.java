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
