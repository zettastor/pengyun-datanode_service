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

public abstract class BasicLog implements DataLog {
  private final long logId;
  private final long offset;
  private final int length;
  private final long logUuid;

  protected BasicLog(long logId, long offset, int length, long logUuid) {
    this.logId = logId;
    this.offset = offset;
    this.length = length;
    this.logUuid = logUuid;
  }

  @Override
  public long getLogId() {
    return logId;
  }

  @Override
  public long getOffset() {
    return offset;
  }

  @Override
  public int getLength() {
    return length;
  }

  @Override
  public long getLogUuid() {
    return logUuid;
  }

  @Override
  public String toString() {
    return "BasicLog{"
        + "logId=" + logId
        + ", logUUID=" + logUuid
        + ", offset=" + offset
        + ", length=" + length
        + ", log status =" + getLogStatus()
        + ", applied=" + isApplied()
        + ", persisted=" + isPersisted()
        + '}';
  }

}
