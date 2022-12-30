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
