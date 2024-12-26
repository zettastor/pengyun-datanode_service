
package py.datanode.archive;

import com.google.common.annotations.VisibleForTesting;
import java.util.List;
import py.archive.brick.BrickMetadata;
import py.archive.page.MultiPageAddress;
import py.archive.page.PageAddress;
import py.storage.Storage;

public interface ShadowPageManager {
  public Storage getStorage();

  public long getSegmentUnitDataStartPosition();

}
