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
