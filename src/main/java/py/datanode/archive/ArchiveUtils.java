
package py.datanode.archive;

import org.apache.commons.lang3.NotImplementedException;
import py.archive.AbstractArchiveBuilder;
import py.archive.ArchiveType;
import py.datanode.configuration.DataNodeConfiguration;
import py.storage.Storage;

public class ArchiveUtils {
  public static AbstractArchiveBuilder getArchiveBuilder(DataNodeConfiguration cfg, Storage storage,
      ArchiveType archiveType) {
    AbstractArchiveBuilder builder;
    switch (archiveType) {
      case RAW_DISK:
        RawArchiveBuilder rawArchiveBuilder = new RawArchiveBuilder(cfg, storage);
        builder = rawArchiveBuilder;
        break;
      case UNSETTLED_DISK:
        builder = new UnsettledArchiveBuild(storage, cfg);
        break;
      default:
        throw new NotImplementedException("not support the archive type" + archiveType);
    }

    return builder;
  }
}
