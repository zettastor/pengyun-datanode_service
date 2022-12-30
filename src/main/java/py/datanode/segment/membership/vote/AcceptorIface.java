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

package py.datanode.segment.membership.vote;

import py.datanode.exception.AcceptedProposalTooOldException;
import py.datanode.exception.AcceptorFrozenException;
import py.datanode.exception.ProposalNumberTooSmallException;
import py.exception.StorageException;

public interface AcceptorIface<N extends Comparable<N>, V> {
  void init();

  void freeze();

  void open();

  V promise(N proposalNum, N minProposalNumOnAcceptedValue)
      throws ProposalNumberTooSmallException, AcceptorFrozenException,
      AcceptedProposalTooOldException, StorageException;

  void accept(N n, V v)
      throws ProposalNumberTooSmallException, AcceptorFrozenException, StorageException;

  N getMaxN();

  N getLastN();

  V getAcceptedProposal();

}
