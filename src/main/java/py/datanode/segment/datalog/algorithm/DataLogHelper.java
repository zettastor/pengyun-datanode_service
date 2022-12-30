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

import py.datanode.segment.datalog.MutationLogEntry;
import py.datanode.segment.datalog.MutationLogEntry.LogStatus;
import py.datanode.service.DataNodeRequestResponseHelper;
import py.thrift.datanode.service.LogThrift;

public class DataLogHelper {
  public static DataLog buildDataLog(LogThrift log) {
    return new ImmutableStatusDataLog(log.getLogId(), log.getLogInfo().getOffset(),
        log.getLogInfo().getLength(), log.getLogUuid(),
        DataNodeRequestResponseHelper.buildLogStatusFrom(log.getStatus()), false, false) {
      @Override
      public byte[] getData() {
        return log.getLogInfo().getData();
      }

      @Override
      public long getCheckSum() {
        return log.getLogInfo().checksum;
      }
    };
  }

  public static DataLog buildDataLog(MutationLogEntry log) {
    return new ImmutableStatusDataLog(log.getLogId(), log.getOffset(),
        log.getLength(), log.getUuid(),  log.getStatus(), false, false) {
      @Override
      public byte[] getData() {
        return log.getData();
      }

      @Override
      public long getCheckSum() {
        return log.getChecksum();
      }

      @Override
      public LogStatus getLogStatus() {
        return log.getStatus();
      }
    };
  }

}
