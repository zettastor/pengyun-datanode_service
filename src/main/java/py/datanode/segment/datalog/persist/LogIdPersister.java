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
