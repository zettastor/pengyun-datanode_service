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

package py.datanode.segment.membership.statemachine;

import py.archive.segment.SegId;
import py.archive.segment.recurring.ContextKey;

/**
 * There are at most one context which is not at primary and might be multiple contexts for the
 * primary status.
 *
 */
public class StateProcessingContextKey extends ContextKey {
  private final String stateProcessingType;

  public StateProcessingContextKey(SegId segId) {
    this(segId, null);
  }

  public StateProcessingContextKey(SegId segId, String stateProcessingType) {
    super(segId);
    this.stateProcessingType = stateProcessingType;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((segId == null) ? 0 : segId.hashCode());
    result = prime * result + ((stateProcessingType == null) ? 0 : stateProcessingType.hashCode());
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

    StateProcessingContextKey other = (StateProcessingContextKey) obj;
    if (segId == null) {
      if (other.segId != null) {
        return false;
      }
    } else if (!segId.equals(other.segId)) {
      return false;
    }

    if (stateProcessingType == null) {
      if (other.stateProcessingType != null) {
        return false;
      }
    } else if (!stateProcessingType.equals(other.stateProcessingType)) {
      return false;
    }

    return true;
  }

  public String getStateProcessingType() {
    return stateProcessingType;
  }

  @Override
  public String toString() {
    return "StateProcessingContextKey [SegId=" + segId + ", stateProcessingType="
        + stateProcessingType + "]";
  }

  public static enum PrimaryProcessingType {
    ExpirationChecker,
    LeaseExtender,
    VolumeMetadataDuplicator,
    Janitor
  }
}
