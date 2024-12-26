

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
