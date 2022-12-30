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

package py.datanode.segment.datalogbak.catchup;

import py.archive.segment.SegId;
import py.archive.segment.recurring.ContextKey;

public class CatchupLogContextKey extends ContextKey {
  private final CatchupLogDriverType type;

  public CatchupLogContextKey(SegId segId, CatchupLogDriverType type) {
    super(segId);
    this.type = type;
  }

  public CatchupLogDriverType getLogDriverType() {
    return type;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((segId == null) ? 0 : segId.hashCode());
    result = prime * result + ((type == null) ? 0 : type.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    CatchupLogContextKey other = (CatchupLogContextKey) obj;
    if (segId == null) {
      if (other.segId != null) {
        return false;
      }
    } else if (!segId.equals(other.segId)) {
      return false;
    }
    if (type != other.type) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return "CatchupLogContextKey [type=" + type + ", toString()=" + super.toString() + "]";
  }

  public static enum CatchupLogDriverType {
    PCL, PLAL, PPL
  }
}
