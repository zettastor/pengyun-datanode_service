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

package py.datanode.segment.copy;

import py.proto.Broadcastlog.PbCopyPageStatus;

public enum CopyPageStatus {
  None(0),

  InitiateCatchupLog(1),

  CatchupLog(2),

  InitiateCopyPage(3),

  CopyPage(4),

  Done(5),

  Abort(6);

  private int value;

  CopyPageStatus(int value) {
    this.value = value;
  }

  public int getValue() {
    return value;
  }

  public PbCopyPageStatus getPbCopyPageStatus() {
    switch (value) {
      case 1:
        return PbCopyPageStatus.COPY_PAGE_PROCESSING;
      case 2:
      case 3:
      case 4:
      case 5:
      default:
        throw new RuntimeException("not the support the value " + value);

    }
  }

  public boolean isCatchingUpLog() {
    return value <= CatchupLog.value;
  }
}
