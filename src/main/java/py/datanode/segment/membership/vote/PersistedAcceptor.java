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

import java.io.Serializable;
import java.util.Objects;
import py.datanode.exception.AcceptedProposalTooOldException;
import py.datanode.exception.AcceptorFrozenException;
import py.datanode.exception.ProposalNumberTooSmallException;
import py.exception.StorageException;

public class PersistedAcceptor<N extends Serializable & Comparable<N>, V extends Serializable>
    implements AcceptorIface<N, V> {
  private final AcceptorPersister<N, V> persister;
  private final Acceptor<N, V> inMemoryAcceptor;

  public PersistedAcceptor(AcceptorPersister<N, V> persister) throws StorageException {
    this.persister = persister;
    this.inMemoryAcceptor = persister.restore();
  }

  private void persist() throws StorageException {
    persister.persist(inMemoryAcceptor);
  }

  @Override
  public synchronized void init() {
    inMemoryAcceptor.init();
  }

  @Override
  public synchronized void freeze() {
    inMemoryAcceptor.freeze();
  }

  @Override
  public synchronized void open() {
    inMemoryAcceptor.open();
  }

  @Override
  public synchronized V promise(N proposalNum, N minProposalNumOnAcceptedValue)
      throws ProposalNumberTooSmallException, AcceptorFrozenException,
      AcceptedProposalTooOldException, StorageException {
    V v = inMemoryAcceptor.promise(proposalNum, minProposalNumOnAcceptedValue);
    persist();
    return v;
  }

  @Override
  public synchronized void accept(N n, V v)
      throws ProposalNumberTooSmallException, AcceptorFrozenException, StorageException {
    inMemoryAcceptor.accept(n, v);
    persist();
  }

  @Override
  public synchronized N getMaxN() {
    return inMemoryAcceptor.getMaxN();
  }

  @Override
  public synchronized N getLastN() {
    return inMemoryAcceptor.getLastN();
  }

  @Override
  public synchronized V getAcceptedProposal() {
    return inMemoryAcceptor.getAcceptedProposal();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PersistedAcceptor<?, ?> that = (PersistedAcceptor<?, ?>) o;
    return Objects.equals(inMemoryAcceptor, that.inMemoryAcceptor);
  }

  @Override
  public int hashCode() {
    return Objects.hash(inMemoryAcceptor);
  }

  @Override
  public String toString() {
    return "PersistedAcceptor{"
        + "persister=" + persister
        + ", inMemoryAcceptor=" + inMemoryAcceptor
        + '}';
  }
}
