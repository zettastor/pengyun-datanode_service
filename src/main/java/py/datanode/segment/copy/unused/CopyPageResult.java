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

package py.datanode.segment.copy.unused;

import py.engine.ResultImpl;

public class CopyPageResult extends ResultImpl {
  private volatile CopyPageTask task;
  private volatile boolean done;

  public CopyPageResult(CopyPageTask task) {
    this(task, null);
  }

  public CopyPageResult(CopyPageTask task, Exception e) {
    super(e);
    this.task = task;
    this.done = false;
  }

  public boolean isDone() {
    return done;
  }

  public void setDone(boolean done) {
    this.done = done;
  }

  public CopyPageTask getCopyPageTask() {
    return task;
  }

  @Override
  public String toString() {
    return "CopyPageResult [super=" + super.toString() + ", task=" + task + "]";
  }
}
