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
