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
