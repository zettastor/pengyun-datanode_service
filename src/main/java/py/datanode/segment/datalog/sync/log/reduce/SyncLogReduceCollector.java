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
