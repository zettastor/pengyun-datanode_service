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

package py.datanode.service;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import org.apache.commons.lang3.Validate;

/**
 * A collection of static helper methods for dealing with memory-mapped page files.
 */
public class PageManagerFileUtils {
  public static final String MEMORY_MAPPED_PAGE_POOL_SUBDIR = "mmpp";
  public static final String MEMORY_MAPPED_PAGE_POOL_ = "mmpp";

  public static String initializeMemoryMappedPagePoolDirectory(File persistenceRoot,
      String mmppSubDir) {
    Validate.isTrue(persistenceRoot != null);
    File dir = new File(persistenceRoot, mmppSubDir);

    if (!dir.exists() && !dir.isDirectory()) {
      if (!dir.mkdirs()) {
        throw new IllegalStateException(
            "could not create directory for memory mapped page pool: " + dir);
      }
    }
    return dir.getPath();
  }

  /**
   * Get the restore directory.
   */
  public static File getPagePoolRestoreDirectory(File persistenceRoot, String mmppRestoreDir) {
    Validate.isTrue(persistenceRoot != null);
    File dir = null;

    dir = new File(persistenceRoot, mmppRestoreDir);

    if (!dir.exists()) {
      dir.mkdir();
    }
    if (!dir.isDirectory()) {
      throw new IllegalStateException(
          "expected page pool restore dir to be a directory! path: " + dir.getAbsolutePath());
    }
    return dir;
  }

  /**
   * Move a file from src to destination, deleting the src file if the bytes are copied
   * successfully.
   */
  public static void moveFile(File src, File dest) throws IOException {
    FileInputStream fin = null;
    FileOutputStream fout = null;
    try {
      fin = new FileInputStream(src);
      FileChannel input = fin.getChannel();
      fout = new FileOutputStream(dest);
      FileChannel output = fout.getChannel();

      long transferred = 0L;
      final long size = input.size();
      while ((size - transferred) > 0) {
        transferred += output.transferFrom(input, transferred, size - transferred);
      }
    } finally {
      fin.close();
      fout.close();
    }

    src.delete();
  }
}
