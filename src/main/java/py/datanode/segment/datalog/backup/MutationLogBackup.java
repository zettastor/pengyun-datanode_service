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

package py.datanode.segment.datalog.backup;

import java.io.IOException;
import java.util.List;
import py.datanode.segment.datalog.MutationLogEntry;
import py.datanode.segment.datalog.SegmentLogMetadata;
import py.exception.NoAvailableBufferException;

public interface MutationLogBackup {
  public void backupMutationLog(SegmentLogMetadata logMetadata) throws Exception;

  public void deleteBackup() throws IOException;

  public boolean readMutationLogs(List<MutationLogEntry> logs, int maxLogNum)
      throws NoAvailableBufferException, IOException;

  public boolean readMutationLogsAfterPcl(List<MutationLogEntry> logs, int maxLogNum)
      throws NoAvailableBufferException, IOException;

  public void open() throws IOException;

  public void close() throws IOException;

}
