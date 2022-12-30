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

public class AppliedLog extends ImmutableStatusDataLog {
  public AppliedLog(long logId, long offset, int length, long logUuid) {
    super(logId, offset, length, logUuid,  LogStatus.Committed, true, false);
  }

  @Override
  public void getData(ByteBuffer destination, int offset, int length) {
    throw new UnsupportedOperationException("you can't get data from an applied log");
  }

  @Override
  public void getData(ByteBuf destination, int offset, int length) {
    throw new UnsupportedOperationException("you can't get data from an applied log");
  }

}
