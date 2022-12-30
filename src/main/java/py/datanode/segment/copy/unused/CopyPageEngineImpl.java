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

package py.datanode.segment.copy.unused;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.engine.SingleTaskEngine;
import py.engine.Task;
import py.token.controller.TokenController;

/**
 * The engine used for pushing page to pre-secondary and controlling its speed.
 */
public class CopyPageEngineImpl extends SingleTaskEngine {
  private static final Logger logger = LoggerFactory.getLogger(CopyPageEngineImpl.class);

  public CopyPageEngineImpl(TokenController controller) {
    setPrefix("primary-copy-page");
    setTokenController(controller);
  }

  @Override
  public boolean drive(Task task) {
    return super.drive(task);
  }

  @Override
  public void stop() {
    try {
      super.stop();
    } catch (Throwable t) {
      logger.warn("caught an exception", t);
    }
  }
}
