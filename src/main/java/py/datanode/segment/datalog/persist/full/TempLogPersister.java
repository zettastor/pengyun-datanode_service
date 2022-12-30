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

package py.datanode.segment.datalog.persist.full;

import java.io.IOException;
import java.util.List;
import py.archive.segment.SegId;
import py.datanode.segment.datalog.MutationLogEntry;
import py.datanode.segment.datalog.MutationLogManager;

/**
 * This class is used to persist logs while writing them to segment log metadata.
 */
public interface TempLogPersister {
  void persistLogs(SegId segId, List<MutationLogEntry> logs) throws IOException;

  public void insertMissingLogs(MutationLogManager manager, boolean cleanLogs);

  public void close() throws IOException;
}
