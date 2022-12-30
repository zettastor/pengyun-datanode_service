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

package py.datanode.segment.datalog.algorithm;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

public class DataLogApplierWithByteBuffer extends DataLogsApplier {
  private ByteBuffer destination;
  private int initPosition;

  public DataLogApplierWithByteBuffer(long destinationPos, int destinationLength, int pageSize) {
    super(destinationPos, destinationLength, pageSize);
  }

  @Override
  protected void applyLogData(DataLog log, int offsetInDestination, int offsetInLog,
      int length) {
    destination.position(initPosition + offsetInDestination);
    log.getData(destination, offsetInLog, length);
  }

  @Override
  protected CompletableFuture<Void> loadPageData(boolean wholePageCovered) {
    return CompletableFuture.completedFuture(null);
  }

  public void setDestination(ByteBuffer destination) {
    if (this.destination != null) {
      throw new IllegalArgumentException("destination already set");
    }
    this.destination = destination;
    this.initPosition = destination.position();
  }
}
