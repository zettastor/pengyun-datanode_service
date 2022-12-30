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
