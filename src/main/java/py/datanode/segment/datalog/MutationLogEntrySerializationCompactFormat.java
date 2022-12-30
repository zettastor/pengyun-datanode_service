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

public class MutationLogEntrySerializationCompactFormat {
  protected static final FieldType[] fieldsInOrderForPersistedLogs = {FieldType.UUID,
      FieldType.LOG_ID,
      FieldType.LOG_STATUS, FieldType.LOG_OFFSET, FieldType.LOG_LENGTH,
      };
  protected static final FieldType[] fieldsInOrderForTempLogs = {FieldType.UUID, FieldType.LOG_ID,
      FieldType.LOG_STATUS, FieldType.LOG_OFFSET, FieldType.LOG_LENGTH,
      FieldType.LOG_CHECKSUM, FieldType.VOLUME_ID, FieldType.SEG_INDEX};
  // it is : long (uuid) + long (log id) + byte(status) + long (offset)
  // + integer(length)
  protected static final int SERIALIZED_LOG_SIZE_FOR_PERSISTED_LOGS =
      8 + 8 + 1 + 8 + 4;
  // it is : long (uuid) + long (log id) + byte(status)
  // + long (offset) + integer(length) + integer(snapshot id) +
  // long(checksum) + long(volume id) + integer(segment index)
  protected static final int SERIALIZED_LOG_SIZE_FOR_TEMP_LOGS =
      8 + 8 + 1 + 8 + 4 + 8 + 8 + 4;
  static final byte LOG_STATUS_COMMITED_FIELD_VALUE = 'c';
  static final byte LOG_STATUS_ABORTED_CONFIRMED_FIELD_VALUE = 'a';
  static final byte LOG_STATUS_CREATED_FIELD_VALUE = 'r';
  static final byte LOG_STATUS_ABORTED_FIELD_VALUE = 'b';

  protected enum FieldType {
    VOLUME_ID, SEG_INDEX, UUID, LOG_ID, LOG_TYPE, LOG_OFFSET, LOG_LENGTH, LOG_CHECKSUM, LOG_STATUS, 
    SNAPSHOT_VERSION, CLONE_VOLUME_DATA_OFFSET, CLONE_VOLUME_DATA_LENGTH
  }
}
