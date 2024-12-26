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
