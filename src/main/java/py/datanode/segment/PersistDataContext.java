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

package py.datanode.segment;

import org.apache.commons.lang.Validate;
import py.archive.AbstractSegmentUnitMetadata;
import py.archive.brick.BrickMetadata;
import py.datanode.archive.RawArchive;

public class PersistDataContext {
  private AbstractSegmentUnitMetadata abstractSegmentUnitMetadata;
  private BrickMetadata brickMetadata;
  private PersistDataType persistWorkType;
  private boolean needBroadcastMembership;
  private RawArchive rawArchive;

  public PersistDataContext(AbstractSegmentUnitMetadata abstractSegmentUnitMetadata,
      PersistDataType persistWorkType, boolean needBroadcastMembership) {
    this(abstractSegmentUnitMetadata, persistWorkType, needBroadcastMembership, null);
  }

  public PersistDataContext(AbstractSegmentUnitMetadata abstractSegmentUnitMetadata,
      PersistDataType persistWorkType, boolean needBroadcastMembership, RawArchive rawArchive) {
    Validate.notNull(persistWorkType);
    this.setAbstractSegmentUnitMetadata(abstractSegmentUnitMetadata);
    this.persistWorkType = persistWorkType;
    this.needBroadcastMembership = needBroadcastMembership;
    this.rawArchive = rawArchive;
  }

  public PersistDataContext(BrickMetadata brickMetadata, PersistDataType persistWorkType,
      RawArchive rawArchive) {
    Validate.notNull(persistWorkType);
    this.brickMetadata = brickMetadata;
    this.persistWorkType = persistWorkType;
    this.needBroadcastMembership = false;
    this.rawArchive = rawArchive;
  }

  public AbstractSegmentUnitMetadata getAbstractSegmentUnitMetadata() {
    return abstractSegmentUnitMetadata;
  }

  public void setAbstractSegmentUnitMetadata(
      AbstractSegmentUnitMetadata abstractSegmentUnitMetadata) {
    this.abstractSegmentUnitMetadata = abstractSegmentUnitMetadata;
  }

  public BrickMetadata getBrickMetadata() {
    return brickMetadata;
  }

  public PersistDataType getPersistWorkType() {
    return persistWorkType;
  }

  public void setPersistWorkType(PersistDataType persistWorkType) {
    this.persistWorkType = persistWorkType;
  }

  public boolean isNeedBroadcastMembership() {
    return needBroadcastMembership;
  }

  public void setNeedBroadcastMembership(boolean needBroadcastMembership) {
    this.needBroadcastMembership = needBroadcastMembership;
  }

  public RawArchive getRawArchive() {
    return rawArchive;
  }

  @Override
  public String toString() {
    return "PersistDataContext{" + "abstractSegmentUnitMetadata=" + abstractSegmentUnitMetadata
        + ", persistWorkType=" + persistWorkType + ", needBroadcastMembership="
        + needBroadcastMembership + '}';
  }
}
