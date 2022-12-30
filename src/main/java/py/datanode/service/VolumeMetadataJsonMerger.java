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
