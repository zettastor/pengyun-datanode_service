

package py.datanode.segment.datalog;

public class MutationLogReaderWriterFactoryForTempStore {
  private int serializedLogSize;

  public MutationLogReaderWriterFactoryForTempStore() {
    serializedLogSize = 
        MutationLogEntrySerializationCompactFormat.SERIALIZED_LOG_SIZE_FOR_TEMP_LOGS;
  }

  public MutationLogEntryWriter generateWriter() {
    return new MutationLogEntryWriterCompactImplForTempLogs();
  }

  public MutationLogEntryReader generateReader() {
    return new MutationLogEntryReaderCompactImplForTempLogs();
  }

  public int getSerializedLogSize() {
    return serializedLogSize;
  }
}
