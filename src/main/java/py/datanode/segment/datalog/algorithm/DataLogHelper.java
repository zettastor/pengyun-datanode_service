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
