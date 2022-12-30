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

package py.datanode.segment.membership.statemachine.checksecondaryinactive;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.Archive;
import py.datanode.checksecondaryinactive.CheckSecondaryInactive;
import py.datanode.checksecondaryinactive.CheckSecondaryInactiveByAbsoluteTime;
import py.datanode.checksecondaryinactive.CheckSecondaryInactiveByRelativeTime;

public class NewSegmentUnitExpirationThreshold {
  private static final Logger logger = LoggerFactory.getLogger(NewSegmentUnitExpirationThreshold.class);
  private static Map<Archive, CheckSecondaryInactive> DiskExpirationThresholdMap = 
      new ConcurrentHashMap<>();

  public static CheckSecondaryInactive getExpirationThreshold(Archive archive) {
    return DiskExpirationThresholdMap.get(archive);
  }

  public static void putExpirationThreshold(Archive archive,
      CheckSecondaryInactive expirationThreshold) {
    DiskExpirationThresholdMap.put(archive, expirationThreshold);
  }

  public static CheckSecondaryInactive getBogusCheckSecondary() {
    return new BogusCheckSecondary();
  }

  public static CheckSecondaryInactive createCheckSecondaryInactiveByAbsoluteTime(
      boolean ignoreMissPagesAndLogs,
      long startTime, long endTime) {
    return new CheckSecondaryInactiveByAbsoluteTime(ignoreMissPagesAndLogs, startTime, endTime);
  }

  public static CheckSecondaryInactive createCheckSecondaryInactiveByRelativeTime(
      boolean ignoreMissPagesAndLogs,
      long waitTimerThreshold) {
    return new CheckSecondaryInactiveByRelativeTime(ignoreMissPagesAndLogs, waitTimerThreshold);
  }

  public static Map<Archive, CheckSecondaryInactive> getDiskExpirationThresholdMap() {
    return DiskExpirationThresholdMap;
  }
}
