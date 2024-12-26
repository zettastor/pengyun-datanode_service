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

package py.datanode.segment.datalog;

import py.datanode.configuration.LogPersistingConfiguration;

public class MutationLogReaderWriterFactory {
  private LogPersistingConfiguration cfg;
  private int serializedLogSize;

  public MutationLogReaderWriterFactory(LogPersistingConfiguration cfg) {
    this.cfg = cfg;

    if (isJsonFormat()) {
      serializedLogSize = MutationLogEntrySerializationJsonFormat.SERIALIZED_LOG_SIZE;
    } else {
      serializedLogSize = 
          MutationLogEntrySerializationCompactFormat.SERIALIZED_LOG_SIZE_FOR_PERSISTED_LOGS;
    }
  }

  private boolean isJsonFormat() {
    return "json".equalsIgnoreCase(cfg.getLogSerializationType().trim());
  }

  public MutationLogEntryWriter generateWriter() {
    if (isJsonFormat()) {
      return new MutationLogEntryWriterJsonImpl();
    } else {
      return new MutationLogEntryWriterCompactImplForPersistedLogs();
    }
  }

  public MutationLogEntryReader generateReader() {
    if (isJsonFormat()) {
      return new MutationLogEntryReaderJsonImpl();
    } else {
      return new MutationLogEntryReaderCompactImplForPersistedLogs();
    }
  }

  public int getSerializedLogSize() {
    return serializedLogSize;
  }
}
