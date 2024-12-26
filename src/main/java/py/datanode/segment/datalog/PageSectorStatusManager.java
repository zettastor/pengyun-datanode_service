/**
* Copyright (C) 2013-2024 Nanjing Pengyun Network Technology Co., Ltd.
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
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
