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
