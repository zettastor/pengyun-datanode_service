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
