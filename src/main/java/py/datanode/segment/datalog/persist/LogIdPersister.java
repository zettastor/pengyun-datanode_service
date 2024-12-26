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

package py.datanode.segment.datalog.persist;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.datanode.exception.LogIdTooSmall;

public class LogIdPersister {
  private static final Logger logger = LoggerFactory.getLogger(LogIdPersister.class);
  private BufferedWriter writer;
  private Long latestPersistedLogId = null;

  public LogIdPersister(String fileName) throws IOException {
    File file = new File(fileName);
    boolean notExists = file.createNewFile();

    if (!notExists) {
      BufferedReader reader = new BufferedReader(new FileReader(file));
      String line = reader.readLine();
      latestPersistedLogId = new Long(line);
    }

    writer = new BufferedWriter(new FileWriter(file));
  }

  public void persistLogId(long id) throws IOException, LogIdTooSmall {
    if (latestPersistedLogId != null && id < latestPersistedLogId.longValue()) {
      logger
          .warn("log id {} is too small to persist. The max log id that has been persisted is {} ",
              id, latestPersistedLogId);
      throw new LogIdTooSmall();
    }

    writer.write(Long.toString(id));
    latestPersistedLogId = id;
  }

  public Long getLatestPersistedLog() {
    return latestPersistedLogId;
  }

  public void close() throws IOException {
    writer.flush();
    writer.close();
  }
}
