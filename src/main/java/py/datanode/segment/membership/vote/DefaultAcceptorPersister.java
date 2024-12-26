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

package py.datanode.segment.membership.vote;

import py.exception.StorageException;
import py.proto.Broadcastlog.PbMembership;

public class DefaultAcceptorPersister implements AcceptorPersister<Integer, PbMembership> {
  @Override
  public Acceptor<Integer, PbMembership> restore() throws StorageException {
    return Acceptor.create(0, 0);
  }

  @Override
  public void persist(Acceptor<Integer, PbMembership> acceptor)
      throws StorageException {
  }
}
