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
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import py.archive.segment.SegId;
import py.common.struct.Pair;

public abstract class MutationLogEntryReader implements Closeable {
  public abstract  void open(InputStream inputStream) throws IOException;

  public abstract  Pair<SegId, MutationLogEntry> readLogAndSegment() throws IOException;

  public abstract  MutationLogEntry read() throws IOException;

  protected abstract MutationLogEntrySerializationCompactFormat.FieldType[] getFormat();

  public abstract DataInputStream getInputStream();
}
