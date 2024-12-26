

package py.datanode;

import py.archive.ArchiveMetadata;

public interface ArchiveBuilderCallback {
  boolean configMatched(ArchiveMetadata archiveMetadata);
}
