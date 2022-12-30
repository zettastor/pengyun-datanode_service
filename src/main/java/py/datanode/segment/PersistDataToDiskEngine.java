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

package py.datanode.segment;

import java.util.concurrent.RejectedExecutionException;

public interface PersistDataToDiskEngine {
  public void asyncPersistData(PersistDataContext context) throws RejectedExecutionException;

  /**
   * Persist the data right away. No rejected exception will be thrown.
   *
   */
  public void syncPersistData(PersistDataContext context);

  public void start() throws Exception;

  public void stop() throws Exception;
}
