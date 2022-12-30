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

public enum PersistDataType {
  SegmentUnitMetadata(1),

  // only for segment unit bitmap, not for shadow unit bitmap
  Bitmap(2),

  ShadowUnitMetadata(3),

  SegmentUnitMetadataAndBitMap(4),

  BrickMetadata(6),

  SegmentUnitMetadataAndBitMapAndCleanSnapshotAndCleanBrickMetadata(7),

  SegmentUnitMetadataAndBrickMetadata(8),

  SegmentUnitMetadataAndBitMapAndBrickMetadata(9);

  private int value;

  PersistDataType(int value) {
    this.value = value;
  }

  public static PersistDataType findByValue(int value) {
    switch (value) {
      case 1:
        return SegmentUnitMetadata;
      case 2:
        return Bitmap;
      case 3:
        return ShadowUnitMetadata;
      case 4:
        return SegmentUnitMetadataAndBitMap;
      case 6:
        return BrickMetadata;
      case 7:
        return SegmentUnitMetadataAndBitMapAndCleanSnapshotAndCleanBrickMetadata;
      case 8:
        return SegmentUnitMetadataAndBrickMetadata;
      case 9:
        return SegmentUnitMetadataAndBitMapAndBrickMetadata;
      default:
        return null;
    }
  }

  public int getValue() {
    return value;
  }
}
