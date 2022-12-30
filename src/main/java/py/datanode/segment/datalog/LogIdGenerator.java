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
