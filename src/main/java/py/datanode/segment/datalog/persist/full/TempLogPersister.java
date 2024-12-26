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
