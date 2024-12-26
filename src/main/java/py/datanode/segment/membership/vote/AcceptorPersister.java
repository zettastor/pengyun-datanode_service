
package py.datanode.segment.membership.vote;

import java.io.Serializable;
import py.exception.StorageException;

public interface AcceptorPersister<N extends Serializable & Comparable<N>, V extends Serializable> {
  Acceptor<N, V> restore() throws StorageException;

  void persist(Acceptor<N, V> acceptor) throws StorageException;

}
