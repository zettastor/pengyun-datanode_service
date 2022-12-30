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

package py.datanode.segment.datalog;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import py.archive.segment.SegId;

public class MutationLogEntrySaveProxy {
  private String rootSaveLogDir;

  public MutationLogEntrySaveProxy(String rootSaveLogDir) {
    this.rootSaveLogDir = rootSaveLogDir;
  }

  public void saveLogs(SegId segId, List<MutationLogEntry> logs) throws IOException {
    MutationLogEntryWriterSaveJsonImpl writer = new MutationLogEntryWriterSaveJsonImpl();
    writer.open(segId, rootSaveLogDir);
    writer.write(logs);
    writer.close();
  }

  public List<MutationLogEntry> loadLogs(SegId segId, Long archiveId) throws IOException {
    MutationLogEntryReaderSaveJsonImpl reader = new MutationLogEntryReaderSaveJsonImpl();
    if (!reader.open(segId, rootSaveLogDir)) {
      return null;
    }
    reader.setArchiveId(archiveId);
    List<MutationLogEntry> logs = reader.readAllLogFromFile();
    reader.close();
    return logs;
  }

  public void deleteFileBySegId(SegId segId) throws IOException {
    String fileName = MutationLogEntrySaveHelper.buildSaveLogFileNameWithSegId(segId);
    Path pathToLogFile = FileSystems.getDefault().getPath(rootSaveLogDir, fileName);
    if (Files.exists(pathToLogFile)) {
      Files.delete(pathToLogFile);
    }
  }

  public String getRootSaveLogDir() {
    return rootSaveLogDir;
  }

}
