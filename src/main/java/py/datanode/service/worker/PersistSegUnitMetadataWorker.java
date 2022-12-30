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

package py.datanode.service.worker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.datanode.archive.RawArchive;
import py.datanode.archive.RawArchiveManager;
import py.periodic.Worker;

public class PersistSegUnitMetadataWorker implements Worker {
  private static final Logger logger = LoggerFactory.getLogger(PersistSegUnitMetadataWorker.class);

  private RawArchiveManager archiveManager;

  public PersistSegUnitMetadataWorker(RawArchiveManager archiveManager) {
    this.archiveManager = archiveManager;
  }

  @Override
  public void doWork() {
    for (RawArchive archive : archiveManager.getRawArchives()) {
      archive.persistBitMapAndSegmentUnitIfNecessary();
    }
  }
}
