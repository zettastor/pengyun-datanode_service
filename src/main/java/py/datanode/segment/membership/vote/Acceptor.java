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

import java.io.Serializable;
import java.util.Objects;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.datanode.exception.AcceptedProposalTooOldException;
import py.datanode.exception.AcceptorFrozenException;
import py.datanode.exception.ProposalNumberTooSmallException;

public class Acceptor<N extends Comparable<N>, V> implements AcceptorIface<N, V>, Serializable {
  private static final Logger logger = LoggerFactory.getLogger(Acceptor.class);
  private V acceptedProposal;
  private N maxN;

  private N lastN;
  private State state;
  private boolean frozen = false;

  private Acceptor(N maxN, N lastN, V acceptedProposal, State state) {
    this.maxN = maxN;
    this.lastN = lastN;
    this.acceptedProposal = acceptedProposal;
    this.state = state;
  }

  public static <N extends Comparable<N>, V> Acceptor<N, V> create(N maxN, N lastN) {
    return new Acceptor<>(maxN, lastN, null, State.INIT);
  }

  public static <N extends Comparable<N>, V> Acceptor<N, V> create(N maxN, N lastN,
      V acceptedProposal, State state) {
    return new Acceptor<>(maxN, lastN, acceptedProposal, state);
  }

  /**
   * initialize the acceptor. We don't change maxN but reset the state and accepted proposal.
   */
  public synchronized void init() {
    acceptedProposal = null;
    state = State.INIT;
    this.lastN = maxN;
    frozen = true;
  }

  /**
   * Freeze this acceptor so that it won't be able to accept any message.
   */
  public synchronized void freeze() {
    logger.warn("freeze acceptor");
    frozen = true;
  }

  @Override
  public synchronized void open() {
    logger.warn("open acceptor");
    frozen = false;
  }

  public synchronized V promise(N proposalNum, N minProposalNumOnAcceptedValue)
      throws ProposalNumberTooSmallException, AcceptorFrozenException,
      AcceptedProposalTooOldException {
    if (maxN.compareTo(proposalNum) >= 0) {
      throw new ProposalNumberTooSmallException("max: " + maxN + " proposal num: " + proposalNum);
    }

    if (frozen) {
      maxN = proposalNum;
      throw new AcceptorFrozenException("max: " + maxN + " proposal num: " + proposalNum);
    }

    if (state == State.INIT || state == State.PROMISED) {
      maxN = proposalNum;
      state = State.PROMISED;
      return null;
    } else if (state == State. ACCEPTED) {
      if (maxN.compareTo(minProposalNumOnAcceptedValue) > 0) {
        maxN = proposalNum;
        Validate.notNull(acceptedProposal);
        return acceptedProposal;
      } else {
        throw new AcceptedProposalTooOldException();
      }
    } else {
      throw new IllegalStateException("unknown state: " + state);
    }
  }

  public synchronized void accept(N n, V value)
      throws ProposalNumberTooSmallException, AcceptorFrozenException {
    if (frozen) {
      throw new AcceptorFrozenException("max: " + maxN + " accepted proposal: " + acceptedProposal);
    }

    if (n.compareTo(maxN) >= 0) {
      acceptedProposal = value;
      maxN = n;
      state = State. ACCEPTED;
    } else {
      logger.info("Can't accept the proposal. max promised N: {} received N: {}", maxN, n);
      throw new ProposalNumberTooSmallException(
          "Can't accept the proposal. max promised N: " + maxN + " received N:" + n);
    }
  }

  public synchronized N getMaxN() {
    return maxN;
  }

  public synchronized N getLastN() {
    return lastN;
  }

  public synchronized V getAcceptedProposal() {
    return acceptedProposal;
  }

  public State getState() {
    return state;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Acceptor<?, ?> acceptor = (Acceptor<?, ?>) o;
    return Objects.equals(acceptedProposal, acceptor.acceptedProposal) 
        && Objects.equals(maxN, acceptor.maxN) 
        && Objects.equals(lastN, acceptor.lastN)
        && state == acceptor.state;
  }

  @Override
  public int hashCode() {
    return Objects.hash(acceptedProposal, maxN, lastN, state);
  }

  @Override
  public String toString() {
    return "Acceptor{"
        + "acceptedProposal=" + acceptedProposal
        + ", maxN=" + maxN
        + ", lastN=" + lastN
        + ", state=" + state
        + '}';
  }

  enum State {
    INIT,
    PROMISED,
    ACCEPTED,
    FROZEN
  }
}
