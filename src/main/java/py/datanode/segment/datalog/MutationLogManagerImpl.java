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

package py.datanode.segment.datalog;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.segment.SegId;
import py.datanode.configuration.DataNodeConfiguration;
import py.datanode.exception.SegmentLogMetadataNotExistException;
import py.datanode.segment.datalogbak.catchup.SegmentLogQualifier;

public class MutationLogManagerImpl implements MutationLogManager {
  private static final Logger logger = LoggerFactory.getLogger(MutationLogManagerImpl.class);
  private Map<SegId, SegmentLogMetadata> segmentLogs;
  private DataNodeConfiguration cfg;
  private int pageSize;

  public MutationLogManagerImpl(DataNodeConfiguration cfg) {
    this.cfg = cfg;
    this.segmentLogs = new HashMap<SegId, SegmentLogMetadata>();
    this.pageSize = cfg.getPageSize();
  }

  @Override
  public synchronized List<SegmentLogMetadata> getSegments(SegmentLogQualifier qualifier) {
    List<SegmentLogMetadata> segmentLogMetadatas = new ArrayList<SegmentLogMetadata>();
    for (Map.Entry<SegId, SegmentLogMetadata> entry : segmentLogs.entrySet()) {
      if (qualifier == null || qualifier.isQaulified(entry.getValue())) {
        segmentLogMetadatas.add(entry.getValue());
      }
    }

    return segmentLogMetadatas;
  }

  /**
   * Before this API is invoked, the segment log metadata corresponding to the specified segId
   * has been created.
   */
  @Override
  public synchronized void setLogIdGenerator(SegId segId, LogIdGenerator generator)
      throws SegmentLogMetadataNotExistException {
    SegmentLogMetadata segmentLogMetadata = segmentLogs.get(segId);
    if (segmentLogMetadata == null) {
      throw new SegmentLogMetadataNotExistException(
          "segment log metadata with id " + segId + " doesn't exist");
    }
    segmentLogMetadata.updateLogIdGenerator(generator);
  }

  @Override
  public synchronized SegmentLogMetadata getSegment(SegId segId) {
    return segmentLogs.get(segId);
  }

  @Override
  public synchronized SegmentLogMetadata createOrGetSegmentLogMetadata(SegId segId,
      int quorumSize) {
    SegmentLogMetadata logMetadata = segmentLogs.get(segId);
    if (logMetadata == null) {
      logMetadata = new SegmentLogMetadata(segId, pageSize, quorumSize,
          Optional.of(cfg.getGiveYouLogIdTimeout()),
          new LogIdWindow(cfg.getMaxLogIdWindowSize(), cfg.getMaxLogIdWindowSplitCount()));

      segmentLogs.put(segId, logMetadata);
    }
    return logMetadata;
  }

  protected int calculateMaxNumberUnappliedLogsInMemory(int pageSize) {
    long jvmMaxMemory = Runtime.getRuntime().maxMemory();
    long numPages = (long) ((double) jvmMaxMemory * 0.8) / (long) pageSize;
    int maxNumUnappliedLogsInMemory = (int) numPages;
    logger.info("the max memory is {} the numPage is {}  max num unapplied logs in memory is {} ",
        jvmMaxMemory,
        numPages, maxNumUnappliedLogsInMemory);

    Validate.isTrue(maxNumUnappliedLogsInMemory > 0);
    return maxNumUnappliedLogsInMemory;
  }

  @Override
  public synchronized void removeSegmentLogMetadata(SegId segId) {
    SegmentLogMetadata segmentLogMetadata = segmentLogs.remove(segId);
    if (segmentLogMetadata != null) {
      segmentLogMetadata.removeAllLog(true);
    }
  }

  @Override
  public void setSegmentLogMetadata(SegmentLogMetadata logMetadata) {
    segmentLogs.put(logMetadata.getSegId(), logMetadata);
  }
}