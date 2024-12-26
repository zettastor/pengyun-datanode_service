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

package py.datanode.segment.datalog.plal.engine;

import py.archive.page.PageAddress;
import py.datanode.page.IoType;
import py.datanode.page.Page;
import py.datanode.page.PageContext;
import py.datanode.page.TaskType;
import py.datanode.page.context.ComparablePageContext;
import py.engine.BogusLatency;
import py.engine.Latency;
import py.storage.Storage;

public class CheckPageContext extends ComparablePageContext<Page> {
  private final PageAddress pageAddress;
  private Latency latency = BogusLatency.DEFAULT;

  public CheckPageContext(PageAddress pageAddress) {
    this.pageAddress = pageAddress;
  }

  @Override
  public Page getPage() {
    return null;
  }

  @Override
  public void setPage(Page page) {
  }

  @Override
  public void waitFor() throws InterruptedException {
  }

  @Override
  public void done() {
  }

  @Override
  public PageAddress getPageAddressForIo() {
    return pageAddress;
  }

  @Override
  public TaskType getTaskType() {
    return null;
  }

  @Override
  public void setTaskType(TaskType taskType) {
  }

  @Override
  public boolean isSuccess() {
    return false;
  }

  @Override
  public Exception getCause() {
    return null;
  }

  @Override
  public void setCause(Exception e) {
  }

  @Override
  public IoType getIoType() {
    return null;
  }

  @Override
  public void setIoType(IoType ioType) {
  }

  @Override
  public void setExpiredTime(long expiredTime) {
  }

  @Override
  public boolean isExpired() {
    return false;
  }

  @Override
  public PageContext<Page> getOriginalPageContext() {
    return null;
  }

  public PageAddress getPageAddressForCompare() {
    return pageAddress;
  }

  @Override
  public String toString() {
    return "CheckPageContext [pageContext=" + pageAddress + "]";
  }

  @Override
  public Storage getStorage() {
    return null;
  }

  @Override
  public void cancel() {
  }

  @Override
  public boolean isCancel() {
    return false;
  }

  @Override
  public Latency getLatency() {
    return latency;
  }

  @Override
  public void setLatency(Latency latency) {
    this.latency = latency;
  }
}
