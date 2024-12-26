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
