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

package py.datanode.segment.membership.statemachine.processors;

import py.archive.segment.SegmentUnitStatus;
import py.archive.segment.recurring.SegmentUnitTaskContext;
import py.datanode.segment.SegmentUnit;
import py.datanode.segment.datalog.SegmentLogMetadata;
import py.datanode.segment.membership.statemachine.StateProcessingContextKey;

/**
 * It contains all information Updater needs to move membership state.
 *
 */
public class StateProcessingContext extends SegmentUnitTaskContext {
  protected final SegmentUnitStatus status;
  protected final SegmentUnit segmentUnit;

  public StateProcessingContext(StateProcessingContextKey k,
      SegmentUnitStatus status,
      SegmentUnit segmentUnit
  ) {
    super(k);
    this.status = status;
    this.segmentUnit = segmentUnit;
  }

  public StateProcessingContext(StateProcessingContext otherContext) {
    super(otherContext);
    this.status = otherContext.status;
    this.segmentUnit = otherContext.segmentUnit;
  }

  public StateProcessingContextKey getKey() {
    return (StateProcessingContextKey) (super.getKey());
  }

  public SegmentUnitStatus getStatus() {
    return status;
  }

  public SegmentUnit getSegmentUnit() {
    return segmentUnit;
  }

  @Override
  public String toString() {
    return "StateProcessingContext [status=" + status + ", " + super.toString() + "]";
  }

  public SegmentLogMetadata getSegmentLogMetadata() {
    return segmentUnit.getSegmentLogMetadata();
  }
}