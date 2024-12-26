

package py.datanode.segment.datalog.plal.engine;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.page.PageAddressImpl;
import py.archive.segment.SegId;
import py.storage.Storage;

public class CheckPageAddress extends PageAddressImpl {
  private static final Logger logger = LoggerFactory.getLogger(CheckPageAddress.class);
  public CountDownLatch countDownLatch = new CountDownLatch(1);

  public CheckPageAddress(SegId segId, Storage storage) {
    super(segId, Long.MIN_VALUE, 0, storage);
  }

  public boolean waitFor(long timeout, TimeUnit timeunit) {
    try {
      countDownLatch.await(timeout, timeunit);
      return true;
    } catch (Exception e) {
      logger.warn("fail to check page address", e);
    }

    return false;
  }

  public void notifyFor() {
    countDownLatch.countDown();
  }
}
