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
