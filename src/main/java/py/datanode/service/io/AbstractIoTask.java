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

package py.datanode.service.io;

import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.NotImplementedException;
import py.engine.Task;

public abstract class AbstractIoTask implements Task {
  private AtomicInteger counter;

  public AtomicInteger getCounter() {
    return counter;
  }

  public int decrement() {
    return counter.decrementAndGet();
  }

  public int increment() {
    return counter.incrementAndGet();
  }

  public Task setCounter(AtomicInteger counter) {
    this.counter = counter;
    return this;
  }

  @Override
  public void cancel() {
    throw new NotImplementedException("it is for write, read, copy");
  }

  @Override
  public boolean isCancel() {
    return false;
  }

  @Override
  public int getToken() {
    return 1;
  }

  @Override
  public void setToken(int token) {
  }

  @Override
  public void destroy() {
    throw new NotImplementedException("it is for write, read, copy");
  }

  public long getDelay(TimeUnit unit) {
    throw new NotImplementedException("it is for write, read, copy");
  }

  public int compareTo(Delayed o) {
    throw new NotImplementedException("it is for write, read, copy");
  }
}
