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

public interface MutationLogEntrySerializationJsonFormat {
  static final byte PADDING_BYTE = '\t';
  static final byte RETURN_CHAR_BYTE = '\r';
  static final byte CHANGE_LINE_CHAR_BYTE = '\n';

  static final String LOG_DATA_TYPE_FIELD_VALUE = "d";
  static final String LOG_MEMBERSHIP_TYPE_FIELD_VALUE = "m";

  static final String LOG_STATUS_COMMITED_FIELD_VALUE = "c";
  static final String LOG_STATUS_ABORTED_CONFIRMED_FIELD_VALUE = "a";

  static final String LOG_UUID_FIELD_NAME = "u";
  static final String LOG_ID_FIELD_NAME = "i";
  static final String LOG_DATA_TYPE_FIELD_NAME = "d";
  static final String LOG_DATA_OFFSET_FIELD_NAME = "o";
  static final String LOG_DATA_LENGTH_FIELD_NAME = "l";
  static final String LOG_DATA_CHECKSUM_FIELD_NAME = "c";
  static final String LOG_STATUS_FIELD_NAME = "s";
  static final String LOG_SNAPSHOT_VERSION_FIELD_NAME = "p";
  static final String LOG_MEMBERSHIP_VERSION_EPOCH_FIELD_NAME = "e";
  static final String LOG_MEMBERSHIP_VERSION_GENERATION_FIELD_NAME = "g";
  static final String LOG_MEMBERSHIP_PRIMARY_FIELD_NAME = "p";

  static final int SERIALIZED_LOG_SIZE = 100;
}
