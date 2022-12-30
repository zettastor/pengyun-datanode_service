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

package py.datanode.segment.datalog.plal.engine;

import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.page.PageAddress;

public class PlalEntry implements Comparable<PlalEntry> {
  private static final Logger logger = LoggerFactory.getLogger(PlalEntry.class);
  private final PageAddress pageAddress;

  public PlalEntry(PageAddress address) {
    Validate.notNull(address);
    this.pageAddress = address;
  }

  public PageAddress getPageAddress() {
    return pageAddress;
  }

  @Override
  public int compareTo(PlalEntry o) {
    if (!this.pageAddress.getStorage().equals(o.getPageAddress().getStorage())) {
      logger
          .error("two page addresses {} and {} are in different storages. They are not comparable.",
              this.pageAddress, o.pageAddress);
      throw new RuntimeException();
    }

    long diff =
        this.pageAddress.getPhysicalOffsetInArchive() - o.pageAddress.getPhysicalOffsetInArchive();
    if (diff > 0) {
      return 1;
    } else if (diff < 0) {
      return -1;
    } else {
      return 0;
    }
  }

  @Override
  public int hashCode() {
    return pageAddress.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    return pageAddress.equals(obj);
  }
}
