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

package py.datanode.segment.datalog.sync.log.reduce;

import py.datanode.service.DataNodeServiceImpl;
import py.instance.InstanceId;
import py.netty.client.GenericAsyncClientFactory;
import py.netty.core.MethodCallback;
import py.netty.datanode.AsyncDataNode.AsyncIface;

public interface SyncLogReduceCollector<U, M, R> {
  /**
   * packing the sending request message unit by destination instance and message type.
   *
   * @return return true if success
   */
  boolean submit(InstanceId destination, U messageUnit);

  /**
   * collect all response unit for the request which has register to this collector at before.
   *
   * @return return true if success
   */
  boolean collect(long requestId, U messageUnit);

  /**
   * register a request for wait response unit collector.
   *
   */
  void register(InstanceId destination, R request, MethodCallback<M> callback);

  void setDataNodeAsyncClientFactory(GenericAsyncClientFactory<AsyncIface> clientFactory);

  void setDataNodeService(DataNodeServiceImpl dataNodeService);
}
