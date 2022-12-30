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

package py.datanode.segment.datalog.persist;

import java.io.IOException;
import java.util.Map;
import py.archive.segment.SegId;
import py.datanode.exception.LogIdNotFoundException;
import py.third.rocksdb.KvStoreException;

public interface LogStorageSystem {
  public LogStorageMetadata getLogStorageToWrite(SegId segId);

  public LogStorageMetadata createLogStorageToWrite(SegId segId, long id,
      LogStorageMetadata originalStorageMetadata) throws IOException;

  public LogStorageMetadata createLogStorageWhenNoLogFileExist(SegId segId, long id)
      throws IOException, KvStoreException;

  public LogStorageMetadata getOrCreateLogStorageToWrite(SegId segId, long id) throws IOException;

  public LogStorageMetadata getLogStorageToRead(SegId segId, long id)
      throws IOException, LogIdNotFoundException;

  public LogStorageMetadata getLatestLogStorageToRead(SegId segId) throws IOException;

  public void removeLogStorage(SegId segId) throws IOException, KvStoreException;

  public Map<SegId, LogStorageMetadata> init() throws IOException, KvStoreException;

  public void close();

}
