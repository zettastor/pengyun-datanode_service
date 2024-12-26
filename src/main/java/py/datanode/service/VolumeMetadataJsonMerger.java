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

package py.datanode.service;

import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.VolumeMetadataJsonParser;

public class VolumeMetadataJsonMerger {
  private static final Logger logger = LoggerFactory.getLogger(VolumeMetadataJsonMerger.class);
  private final VolumeMetadataJsonParser parserForCurrentVm;
  private List<VolumeMetadataJsonParser> parsersForOthers = new ArrayList<>();

  public VolumeMetadataJsonMerger(String volumeMetadataJson) {
    parserForCurrentVm = new VolumeMetadataJsonParser(volumeMetadataJson);
  }

  public void add(String json) {
    logger.debug("adding {} to vmMerger", json);
    parsersForOthers.add(new VolumeMetadataJsonParser(json));
  }

  /**
   * Merge volume meta.
   */
  public String merge() {
    String jsonHavingHighestVersion = parserForCurrentVm.getCompositedVolumeMetadataJson();
    int highestVersion = parserForCurrentVm.getVersion();
    for (VolumeMetadataJsonParser parserForOthers : parsersForOthers) {
      if (parserForOthers.getVersion() > highestVersion) {
        jsonHavingHighestVersion = parserForOthers.getCompositedVolumeMetadataJson();
        highestVersion = parserForOthers.getVersion();
      }
    }

    return highestVersion >= parserForCurrentVm.getVersion() ? jsonHavingHighestVersion : null;
  }

}
