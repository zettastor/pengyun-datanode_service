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

package py.datanode.segment.datalog;

import py.common.Constants;

public class PageSectorStatusManager {
  private long pageStartOffset;
  private boolean[] sectors;
  private int dirtySectorsCount;

  public PageSectorStatusManager(long pageStartOffset, int sectorCountInOnPage) {
    this.pageStartOffset = pageStartOffset;
    this.sectors = new boolean[sectorCountInOnPage];
    this.dirtySectorsCount = 0;
  }

  public boolean allSectorsDirty() {
    return sectors.length == dirtySectorsCount;
  }

  public void updateSectorStatus(long offset, int length) {
    if (offset < pageStartOffset) {
      throw new IllegalArgumentException(
          "pageStartOffset: " + pageStartOffset + ", offset: " + offset);
    }

    int offsetInOnePage = (int) (offset - pageStartOffset);
    if (offsetInOnePage % Constants.SECTOR_SIZE != 0) {
      return;
    }

    if (length % Constants.SECTOR_SIZE != 0) {
      return;
    }

    int offsetSectorsInOnePage = offsetInOnePage / Constants.SECTOR_SIZE;
    int sectorCountToDealWith = length / Constants.SECTOR_SIZE;

    if (offsetSectorsInOnePage + sectorCountToDealWith > sectors.length) {
      throw new IllegalArgumentException(
          "offset sector: " + offsetSectorsInOnePage + ", deal with sector: "
              + sectorCountToDealWith
              + ", expected: " + sectors.length);
    }

    for (int i = 0; i < sectorCountToDealWith; i++) {
      if (!sectors[offsetSectorsInOnePage + i]) {
        sectors[offsetSectorsInOnePage + i] = true;
        dirtySectorsCount++;
      }
    }
  }

  public boolean isDirty(int index) {
    return sectors[index];
  }
}
