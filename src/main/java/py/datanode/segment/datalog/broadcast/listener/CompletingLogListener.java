

package py.datanode.segment.datalog.broadcast.listener;

public interface CompletingLogListener {
  /**
   * when the completing log get completed.
   *
   * @param logUuid the uuid of the completed log
   */
  public void complete(long logUuid);

  /**
   * the completing log ends up failed.
   *
   * @param logUuid the uuid of the incomplete log
   */
  public void fail(long logUuid, Exception e);
}
