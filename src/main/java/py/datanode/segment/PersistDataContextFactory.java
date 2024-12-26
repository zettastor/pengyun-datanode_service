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

import py.archive.AbstractSegmentUnitMetadata;
import py.datanode.archive.RawArchive;

public class PersistDataContextFactory {
  public static PersistDataContext generatePersistDataContext(
      AbstractSegmentUnitMetadata abstractSegmentUnitMetadata,
      PersistDataType workType) {
    PersistDataContext context = new PersistDataContext(abstractSegmentUnitMetadata, workType,
        false);
    return context;
  }

  public static PersistDataContext generatePersistDataContext(
      AbstractSegmentUnitMetadata abstractSegmentUnitMetadata,
      PersistDataType workType, RawArchive rawArchive) {
    PersistDataContext context = new PersistDataContext(abstractSegmentUnitMetadata, workType,
        false, rawArchive);
    return context;
  }

}
