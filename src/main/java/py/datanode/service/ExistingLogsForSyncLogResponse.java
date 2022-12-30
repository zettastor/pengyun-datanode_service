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

package py.datanode.service;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.datanode.segment.datalog.MutationLogEntry.LogStatus;

public class ExistingLogsForSyncLogResponse {
  private static final Logger logger = LoggerFactory
      .getLogger(ExistingLogsForSyncLogResponse.class);

  private static final int SINGLE_BITMAP_SIZE = Long.SIZE;

  private static final int SINGLE_BITMAP_SIZE_LOG2 = Integer
      .numberOfTrailingZeros(SINGLE_BITMAP_SIZE);

  /**
   * Num of used bits.
   */
  private int numConfirmedLogs = 0;

  /**
   * Max num of bits.
   */
  private int capacity = 0;

  /**
   * Current position of bit map.
   */
  private int position = 0;

  /**
   * Bit map for log ids in request. If the log in request exists in primary, set the bit at the
   * position same as log list in request.
   */
  private List<Long> bitMapForLogIds = new ArrayList<Long>();

  /**
   * Bit map for committed log in request. If the log in request is committed in primary, set the
   * bit at the position same as log list in request.
   */
  private List<Long> bitMapForCommitted = new ArrayList<Long>();

  /**
   * Bit map for aborted-confirmed log in request. If the log in request is aborted-confirmed in
   * primary, set the bit at the position same as log list in request.
   */
  private List<Long> bitMapForAbortedConfirmed = new ArrayList<Long>();

  public ExistingLogsForSyncLogResponse(int capacity, boolean confirmed) {
    increaseBitMapLenIfShort(capacity);

    if (confirmed) {
      confirmed(capacity);
    }
  }

  private static int divideBySingleBitMapSize(int value) {
    return value >> SINGLE_BITMAP_SIZE_LOG2;
  }

  private static int modBySingleBitMapSize(int value) {
    return value & ((1 << SINGLE_BITMAP_SIZE_LOG2) - 1);
  }

  public int capacity() {
    return capacity;
  }

  public int sizeOfConfirmedLogs() {
    return numConfirmedLogs;
  }

  public void confirmed(int size) {
    this.numConfirmedLogs = size;
  }

  public void seek(int position) {
    this.position = position;
  }

  public boolean noConfirmedLogs() {
    return sizeOfConfirmedLogs() == 0;
  }

  public void markLogExisting() {
    setBit(bitMapForLogIds, position);

    ++position;
    numConfirmedLogs = Math.max(numConfirmedLogs, position);
  }

  public void markLogMissing() {
    resetBit(bitMapForLogIds, position);

    ++position;
    numConfirmedLogs = Math.max(numConfirmedLogs, position);
  }

  public boolean exist(int position) {
    if (noConfirmedLogs() || position >= sizeOfConfirmedLogs()) {
      logger.error("Unable to check if log exist at position {} beyond size of confirmed logs {}",
          position,
          sizeOfConfirmedLogs());
      Validate.isTrue(false);
    }

    return getBit(bitMapForLogIds, position);
  }

  public void setLogStatus(int position, LogStatus status) {
    Validate.isTrue(exist(position));

    switch (status) {
      case Committed:
        setCommitted(position);
        break;
      case AbortedConfirmed:
        setAbortedConfirmed(position);
        break;
      default:
        throw new IllegalArgumentException("Invalid status");
    }
  }

  public LogStatus getLogStatus(int position) {
    Validate.isTrue(exist(position));

    if (getBit(bitMapForCommitted, position)) {
      return LogStatus.Committed;
    } else if (getBit(bitMapForAbortedConfirmed, position)) {
      return LogStatus.AbortedConfirmed;
    } else {
      logger.error("Unable to get log status");
      throw new RuntimeException();
    }
  }

  public void setCommitted(int position) {
    setBit(bitMapForCommitted, position);
  }

  public void setAbortedConfirmed(int position) {
    setBit(bitMapForAbortedConfirmed, position);
  }

  public List<Long> getBitMapForLogIds() {
    return bitMapForLogIds;
  }

  public void setBitMapForLogIds(List<Long> bitMapForLogIds) {
    this.bitMapForLogIds = bitMapForLogIds;
  }

  public List<Long> getBitMapForCommitted() {
    return bitMapForCommitted;
  }

  public void setBitMapForCommitted(List<Long> bitMapForCommitted) {
    this.bitMapForCommitted = bitMapForCommitted;
  }

  public List<Long> getBitMapForAbortedConfirmed() {
    return bitMapForAbortedConfirmed;
  }

  public void setBitMapForAbortedConfirmed(List<Long> bitMapForAbortedConfirmed) {
    this.bitMapForAbortedConfirmed = bitMapForAbortedConfirmed;
  }

  private void increaseBitMapLenIfShort(int bitMapLen) {
    if (bitMapLen <= capacity) {
      return;
    }

    while (bitMapLen > capacity) {
      bitMapForLogIds.add(0L);
      bitMapForCommitted.add(0L);
      bitMapForAbortedConfirmed.add(0L);
      capacity += SINGLE_BITMAP_SIZE;
    }
  }

  private void setBit(List<Long> bitMap, int position) {
    if (!checkInBound(position)) {
      logger.error("position {} is out of bound {}", position, capacity);
      throw new IndexOutOfBoundsException();
    }

    int bitMapIndex = divideBySingleBitMapSize(position);
    int singleBitMapOffset = modBySingleBitMapSize(position);

    long singleBitMap = bitMap.get(bitMapIndex);
    singleBitMap |= (1L << singleBitMapOffset);
    bitMap.set(bitMapIndex, singleBitMap);
  }

  private void resetBit(List<Long> bitMap, int position) {
    if (!checkInBound(position)) {
      logger.error("position {} is out of bound {}", position, capacity);
      throw new IndexOutOfBoundsException();
    }

    int bitMapIndex = divideBySingleBitMapSize(position);
    int singleBitMapOffset = modBySingleBitMapSize(position);

    long singleBitMap = bitMap.get(bitMapIndex);
    singleBitMap &= ~(1L << singleBitMapOffset);
    bitMap.set(bitMapIndex, singleBitMap);
  }

  private boolean getBit(List<Long> bitMap, int position) {
    if (!checkInBound(position)) {
      logger.error("position {} is out of bound {}", position, capacity);
      throw new IndexOutOfBoundsException();
    }
    int bitMapIndex = divideBySingleBitMapSize(position);
    int singleBitMapOffset = modBySingleBitMapSize(position);

    long singleBitMap = bitMap.get(bitMapIndex);
    singleBitMap &= (1L << singleBitMapOffset);

    return singleBitMap != 0;
  }

  private boolean checkInBound(int position) {
    return position < capacity;
  }
}
