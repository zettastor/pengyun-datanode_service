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
