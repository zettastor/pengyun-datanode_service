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

package py.datanode.service.io;

import java.nio.channels.CompletionHandler;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.brick.BrickMetadata;
import py.archive.segment.SegmentUnitMetadata;
import py.archive.segment.SegmentUnitType;
import py.consumer.ConsumerService;
import py.datanode.segment.SegmentUnit;

/**
 * persist segment units metadata after segment units has been created in archive.
 *
 */
public class SegmentUnitMetadataWriteTask extends IoTask implements
    Comparable<SegmentUnitMetadataWriteTask> {
  private static final Logger logger = LoggerFactory.getLogger(SegmentUnitMetadataWriteTask.class);
  private final ConsumerService<SegmentUnitMetadataWriteTask> consumerService;
  private CompletionHandler<Integer, SegmentUnit> completionHandler;
  private TaskType taskType;

  public SegmentUnitMetadataWriteTask(
      ConsumerService<SegmentUnitMetadataWriteTask> consumerService, SegmentUnit segmentUnit,
      CompletionHandler<Integer, SegmentUnit> completionHandler, TaskType taskType) {
    super(segmentUnit);
    this.consumerService = consumerService;
    this.completionHandler = completionHandler;
    this.taskType = taskType;
  }

  public SegmentUnitMetadataWriteTask(
      ConsumerService<SegmentUnitMetadataWriteTask> consumerService, SegmentUnit segmentUnit,
      CompletionHandler<Integer, SegmentUnit> completionHandler) {
    this(consumerService, segmentUnit, completionHandler, TaskType.brickMetadata);
  }

  @Override
  public void run() {
    try {
      if (taskType.equals(TaskType.brickMetadata)) {
        processPersistBrickMetadata();
      } else {
        processPersistSegmentUnitMetadata();
      }
    } catch (Throwable t) {
      logger.warn("caught a throwable", t);
      failToProcess(t);
    }
  }

  protected void processPersistBrickMetadata() throws Exception {
    if (segmentUnit.getSegmentUnitMetadata().getSegmentUnitType().equals(SegmentUnitType.Normal)) {
      CompletionHandler<Integer, BrickMetadata> integerSegmentUnitMetadataCompletionHandler =
          new CompletionHandler<Integer, BrickMetadata>() {
            @Override
            public void completed(Integer result, BrickMetadata attachment) {
              SegmentUnitMetadataWriteTask task = new SegmentUnitMetadataWriteTask(
                  consumerService, segmentUnit, completionHandler, TaskType.segmentUnitMetadata
              );
              consumerService.submit(task);
            }

            @Override
            public void failed(Throwable exc, BrickMetadata attachment) {
              completionHandler.failed(exc, getSegmentUnit());
            }
          };

      getSegmentUnit().getArchive()
          .persisBrickMetaAndBitmap(segmentUnit.getSegmentUnitMetadata().getBrickMetadata(),
              integerSegmentUnitMetadataCompletionHandler);
    } else {
      processPersistSegmentUnitMetadata();
    }
  }

  protected void processPersistSegmentUnitMetadata() throws Exception {
    CompletionHandler<Integer, SegmentUnitMetadata> integerSegmentUnitMetadataCompletionHandler =
        new CompletionHandler<Integer, SegmentUnitMetadata>() {
          @Override
          public void completed(Integer result, SegmentUnitMetadata attachment) {
            completionHandler.completed(0, getSegmentUnit());
          }

          @Override
          public void failed(Throwable exc, SegmentUnitMetadata attachment) {
            completionHandler.failed(exc, getSegmentUnit());
          }
        };

    getSegmentUnit().getArchive()
        .persistSegmentUnitMetaAndBitmapAndCleanSnapshot(getSegmentUnit(),
            integerSegmentUnitMetadataCompletionHandler);
  }

  public void failToProcess(Throwable e) {
    completionHandler.failed(e, getSegmentUnit());
  }

  @Override
  public int compareTo(@Nonnull SegmentUnitMetadataWriteTask o) {
    long offset1 = this.getSegmentUnit().getSegmentUnitMetadata().getMetadataOffsetInArchive();
    if (taskType.equals(TaskType.brickMetadata) && this.getSegmentUnit()
        .getSegmentUnitMetadata().getSegmentUnitType().equals(SegmentUnitType.Normal)) {
      offset1 = this.getSegmentUnit().getSegmentUnitMetadata().getBrickMetadata()
          .getMetadataOffsetInArchive();
    }

    long offset2 = o.getSegmentUnit().getSegmentUnitMetadata().getMetadataOffsetInArchive();

    if (o.taskType.equals(TaskType.brickMetadata) && o.getSegmentUnit().getSegmentUnitMetadata()
        .getSegmentUnitType().equals(SegmentUnitType.Normal)) {
      offset2 = this.getSegmentUnit().getSegmentUnitMetadata().getBrickMetadata()
          .getMetadataOffsetInArchive();
    }

    return offset1 == offset2 ? 0 : (offset1 > offset2 ? 1 : -1);
  }

  private enum TaskType {
    brickMetadata,
    segmentUnitMetadata,
  }
}
