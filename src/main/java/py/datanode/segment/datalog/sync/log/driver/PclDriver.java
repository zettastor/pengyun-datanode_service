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

package py.datanode.segment.datalog.sync.log.driver;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;
import py.datanode.segment.membership.Lease;

public abstract class PclDriver implements Callable<Boolean> {
  private final int timeoutForDriverWitchNoEventDriver;
  private Lease myLease;
  private AtomicReference<PclDriverStatus> status = new AtomicReference<>();

  public PclDriver(int timeout) {
    timeoutForDriverWitchNoEventDriver = timeout;
    myLease = new Lease();
    myLease.extend(timeoutForDriverWitchNoEventDriver);
    this.status.set(PclDriverStatus.Free);
  }

  public PclDriverStatus getStatus() {
    return status.get();
  }

  public void setStatus(PclDriverStatus status) {
    this.status.set(status);
  }

  public boolean setStatusWithCheck(PclDriverStatus oldStatus, PclDriverStatus newStatus) {
    if (status.compareAndSet(oldStatus, newStatus)) {
      return true;
    }

    return false;
  }

  public void updateLease() {
    myLease.extendForce();
  }

  protected boolean expire() {
    return myLease.expire();
  }

}
