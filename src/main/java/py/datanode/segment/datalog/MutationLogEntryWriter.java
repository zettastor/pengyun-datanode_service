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

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import py.archive.segment.SegId;

/**
 * A writer to write a mutation log entry to an output stream.
 *
 */
public abstract class MutationLogEntryWriter implements Closeable {
  /**
   * Open the writer.
   *
   */
  public abstract void open(OutputStream outputStream) throws IOException;

  public abstract int write(MutationLogEntry log) throws IOException;

  /**
   * write a log.
   *
   */
  public abstract int write(SegId segId, MutationLogEntry log) throws IOException;

  protected abstract ByteBuffer putLogHeaders(SegId segId, MutationLogEntry log);

  public abstract OutputStream getOutputStream();
}
