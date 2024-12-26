
package py.datanode.exception;

import py.archive.page.PageAddress;

public class PageExistException extends Exception {
  private final PageAddress pageAddress;

  public PageExistException(PageAddress pageAddress) {
    this.pageAddress = pageAddress;
  }

  public PageAddress getPageAddress() {
    return pageAddress;
  }
}
