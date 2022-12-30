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

package py.datanode.segment.copy;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.page.PageAddress;
import py.archive.segment.SegId;

public class CopyPage {
  private static final Logger logger = LoggerFactory.getLogger(CopyPage.class);

  private final PageAddress logicalPageAddress;
  private final int pageIndex;
  private final List<PageNode> pageNodes;
  private boolean done;
  private Itr iterator;
  private volatile long maxLogId = -1;

  public CopyPage(int pageIndex, PageAddress logicalPageAddress) {
    this.pageIndex = pageIndex;
    this.logicalPageAddress = logicalPageAddress;
    this.pageNodes = new ArrayList<>();
    iterator = new Itr();
  }

  public void addOriginalPageNode(PageAddress physicalPageAddress) {
    if (size() > 0) {
      logger.warn("origin snap already exist {}", this);
      return;
    }
    pageNodes.add(new PageNode(physicalPageAddress));
  }

  public int size() {
    return this.pageNodes.size();
  }

  public SegId getSegId() {
    return logicalPageAddress.getSegId();
  }

  public boolean hasNext() {
    return iterator.hasNext();
  }

  public PageNode moveToNext() {
    return iterator.next();
  }

  public void setDone() {
    done = true;
  }

  public boolean isDone() {
    return done;
  }

  public PageNode content() {
    return iterator.current();
  }

  public Iterator<PageNode> iterator() {
    return new Itr();
  }

  public int getPageIndex() {
    return pageIndex;
  }

  public PageAddress getLogicalPageAddress() {
    return logicalPageAddress;
  }

  public long getMaxLogId() {
    return maxLogId;
  }

  public void setMaxLogId(long maxLogId) {
    this.maxLogId = maxLogId;
  }

  public boolean isFirstPageNode() {
    return content() == pageNodes.get(0);
  }

  public boolean isLastOriginPageNode() {
    return !hasNext();
  }

  @Override
  public String toString() {
    return "CopyPage [originalPageAddress=" + logicalPageAddress + ", pageIndex=" + pageIndex
        + ", pageNodes=" + pageNodes + " cursor" + content() + ", maxLogId=" + maxLogId
        + ", done=" + done + "]";
  }

  public class PageNode {
    public final PageAddress physicalPageAddress;
    public volatile ByteBuffer buffer;

    public PageNode(PageAddress physicalPageAddress) {
      this.physicalPageAddress = physicalPageAddress;
      this.buffer = null;
    }

    public boolean pageLoaded() {
      return buffer != null;
    }

    public void release() {
      buffer = null;
    }

    @Override
    public String toString() {
      return "PageNode [pageAddress=" + physicalPageAddress + ", buffer=" + buffer + "]";
    }
  }

  class Itr implements Iterator<PageNode> {
    int cursor = 0;
    PageNode prev;

    Itr() {
    }

    @Override
    public boolean hasNext() {
      return cursor != size();
    }

    @Override
    public PageNode next() {
      int i = cursor;
      if (i >= size()) {
        throw new NoSuchElementException();
      }
      logger.debug("begin move to next, current is {}", prev);
      prev = pageNodes.get(i);
      cursor++;
      logger.debug("last move to next {}", prev);
      return prev;
    }

    public PageNode current() {
      return prev;
    }
  }
}
