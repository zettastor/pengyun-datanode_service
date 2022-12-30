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
import py.datanode.page.Page;
import py.datanode.page.PageManager;
import py.netty.datanode.AsyncDataNode;

public interface PrimaryCopyPageManager extends CopyPageManager {
  PageManager<Page> getPageManager();

  AsyncDataNode.AsyncIface getSecondaryClient();

  void incErrCount();

  boolean reachMaxErrCount();

  long getSessionId();

  ByteBuffer getFromBufferPool(int workerId, int pageIndex);

  void updateNextCopyPageUnit(int workerId, byte[] bitArray, int unitPos);

  void allocatePageAddressAtTheFirstTime(CopyPage[] copyPages, int workerId) throws Exception;
}
