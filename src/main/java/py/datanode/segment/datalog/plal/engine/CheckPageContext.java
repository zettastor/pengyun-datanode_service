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
