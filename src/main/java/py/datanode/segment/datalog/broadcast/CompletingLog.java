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

package py.datanode.segment.datalog.broadcast;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.lang3.Validate;
import py.datanode.segment.datalog.MutationLogEntry;
import py.datanode.segment.datalog.broadcast.exception.HeIsBrokenLogException;
import py.datanode.segment.datalog.broadcast.exception.ImBrokenLogException;
import py.datanode.segment.datalog.broadcast.exception.WeDontHaveLogEntry;
import py.datanode.segment.datalog.broadcast.exception.WeDontHaveLogId;
import py.datanode.segment.datalog.broadcast.listener.CompletingLogListener;

public abstract class CompletingLog {
  protected final long logUuid;
  private final AtomicBoolean done = new AtomicBoolean(false);
  private final CompletableFuture<Boolean> future = new CompletableFuture();
  private final long logCreatedTime;
  protected CompletingLogListener listener;

  CompletingLog(long logUuid, CompletingLogListener listener) {
    this.logUuid = logUuid;
    this.listener = listener;
    logCreatedTime = System.currentTimeMillis();
  }

  public final long getUuid() {
    return logUuid;
  }

  public CompletingLogListener getListener() {
    return listener;
  }

  public final CompleteLog tryComplete(CompletingLog another)
      throws ImBrokenLogException, HeIsBrokenLogException, WeDontHaveLogEntry, WeDontHaveLogId {
    if (another instanceof BrokenLogWithoutLogId) {
      return tryComplete((BrokenLogWithoutLogId) another);
    } else if (another instanceof LogWithoutLogId) {
      return tryComplete((LogWithoutLogId) another);
    } else if (another instanceof LogWithoutLogEntry) {
      return tryComplete((LogWithoutLogEntry) another);
    } else {
      throw new IllegalArgumentException();
    }
  }

  public abstract CompleteLog tryComplete(LogWithoutLogId another)
      throws ImBrokenLogException, WeDontHaveLogId;

  public abstract CompleteLog tryComplete(LogWithoutLogEntry another)
      throws ImBrokenLogException, WeDontHaveLogEntry;

  public CompleteLog tryComplete(BrokenLogWithoutLogId another)
      throws ImBrokenLogException, HeIsBrokenLogException {
    throw new HeIsBrokenLogException();
  }

  public CompleteLog tryComplete(MutationLogEntry log) {
    CompleteLog completeLog = new CompleteLog(log, listener);
    completeLog.getFuture().thenAccept((value) -> {
      this.getFuture().complete(value);
    }).exceptionally(t -> {
      this.getFuture().completeExceptionally(t);
      return null;
    });
    return completeLog;
  }

  final CompleteLog mergeLog(LogWithoutLogEntry logWithoutLogEntry,
      LogWithoutLogId logWithoutLogId) {
    MutationLogEntry log = logWithoutLogId.getLogEntry();
    Validate.isTrue(log.getUuid() == logUuid);

    log.setLogId(logWithoutLogEntry.getLogId());
    CompleteLog completeLog = new CompleteLog(log, logWithoutLogEntry.listener);
    completeLog.getFuture().thenAccept((value) -> {
      logWithoutLogEntry.getFuture().complete(value);
      logWithoutLogId.getFuture().complete(value);
    }).exceptionally(t -> {
      logWithoutLogEntry.getFuture().completeExceptionally(t);
      logWithoutLogId.getFuture().completeExceptionally(t);
      return null;
    });
    return completeLog;
  }

  /**
   * the log failed to be completed.
   *
   * @param e the cause of the failure
   */
  public boolean fail(Exception e) {
    future.completeExceptionally(e);
    if (setDone() && listener != null) {
      listener.fail(logUuid, e);
      return true;
    } else {
      return false;
    }
  }

  public boolean complete() {
    future.complete(true);
    if (setDone() && listener != null) {
      listener.complete(logUuid);
      return true;
    } else {
      return false;
    }
  }

  public boolean setDone() {
    return done.compareAndSet(false, true);
  }

  public boolean hasDone() {
    return done.get();
  }

  public CompletableFuture<Boolean> getFuture() {
    return future;
  }

  public long getLogCreatedTime() {
    return logCreatedTime;
  }

  @Override
  public String toString() {
    return "CompletingLog{"
        + "logUUID=" + logUuid
        + ", listener=" + listener
        + ", done=" + done.get()
        + ", logCreatedTime=" + logCreatedTime
        + ", currentTime=" + System.currentTimeMillis()
        + '}';
  }
}
