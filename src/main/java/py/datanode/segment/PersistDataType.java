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
