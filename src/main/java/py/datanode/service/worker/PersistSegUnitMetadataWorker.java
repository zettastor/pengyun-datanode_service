
package py.datanode.service.worker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.datanode.archive.RawArchive;
import py.datanode.archive.RawArchiveManager;
import py.periodic.Worker;

public class PersistSegUnitMetadataWorker implements Worker {
  private static final Logger logger = LoggerFactory.getLogger(PersistSegUnitMetadataWorker.class);

  private RawArchiveManager archiveManager;

  public PersistSegUnitMetadataWorker(RawArchiveManager archiveManager) {
    this.archiveManager = archiveManager;
  }

  @Override
  public void doWork() {
    for (RawArchive archive : archiveManager.getRawArchives()) {
      archive.persistBitMapAndSegmentUnitIfNecessary();
    }
  }
}
