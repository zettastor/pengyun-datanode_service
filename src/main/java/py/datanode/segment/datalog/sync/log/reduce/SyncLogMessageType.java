
package py.datanode.segment.datalog.sync.log.reduce;

public enum SyncLogMessageType {
  SYNC_LOG_MESSAGE_TYPE_SYNC_LOG_BATCH_REQUEST,
  SYNC_LOG_MESSAGE_TYPE_BACKWARD_REQUEST,
  SYNC_LOG_MESSAGE_TYPE_BACKWARD_RESPONSE {
    @Override
    public boolean isWaitingResponseType() {
      return true;
    }
  };

  public boolean isWaitingResponseType() {
    return false;
  }
}
