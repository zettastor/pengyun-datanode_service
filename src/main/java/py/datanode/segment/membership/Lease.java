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

package py.datanode.segment.membership;

import java.text.SimpleDateFormat;
import java.util.Date;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The class indicates whether the member's lease information.
 */
public class Lease {
  private static final Logger logger = LoggerFactory.getLogger(Lease.class);
  private static final SimpleDateFormat dt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
  private static final long ONE_CENTURY_AGO = 100L * 365L * 24L * 3600L * 1000L;
  private long expirationTime;
  private Long leaseSpan;

  public Lease() {
    init();
  }

  public synchronized boolean expire() {
    long timePassedExpirationTime = expirationTime - System.currentTimeMillis();
    logger.debug("Lease is going to expire in {} ms. ", timePassedExpirationTime);
    if (timePassedExpirationTime < 0) {
      logger.debug("stack trace to show who cares about lease expiration", new Exception());
      logger.info("lease has expired {} lease span is {}", timePassedExpirationTime, leaseSpan);
      return true;
    } else {
      return false;
    }
  }

  /**
   * start the lease.
   */
  public synchronized void start(long spanMs) {
    logger.debug("start the lease and lease span is {} seconds", spanMs);
    init();
    extend(spanMs);
  }

  public synchronized boolean extend() {
    return extend(false);
  }

  public boolean extend(boolean force) {
    if (leaseSpan != null && (force || !expire())) {
     
      expirationTime = System.currentTimeMillis() + leaseSpan;
      return true;
    } else {
     
     
      return false;
    }
  }
  
  
  public synchronized boolean extend(long spanMs) {
    leaseSpan = spanMs;
    return extend(false);
  }

  public synchronized void clear() {
    init();
   
   
  }

  public synchronized void extendForce() {
    extend(true);
  }

  public long getExpirationTime() {
    return expirationTime;
  }

  @Override
  public String toString() {
    return "Lease [expirationTime=" + dt.format(new Date(expirationTime)) + ", leaseSpan="
        + leaseSpan + "]";
  }

  private void init() {
   
    expirationTime = System.currentTimeMillis() + ONE_CENTURY_AGO;
    leaseSpan = null;
  }
}
