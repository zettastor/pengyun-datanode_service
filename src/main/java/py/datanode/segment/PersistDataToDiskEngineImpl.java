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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.ByteBuffer;
import java.nio.channels.CompletionHandler;
import java.util.Comparator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.commons.lang.Validate;
import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.AbstractSegmentUnitMetadata;
import py.archive.ArchiveOptions;
import py.archive.brick.BrickMetadata;
import py.archive.segment.SegId;
import py.archive.segment.SegmentUnitMetadata;
import py.archive.segment.SegmentUnitStatus;
import py.archive.segment.SegmentUnitType;
import py.client.thrift.GenericThriftClientFactory;
import py.common.NamedThreadFactory;
import py.common.struct.EndPoint;
import py.common.tlsf.bytebuffer.manager.TlsfByteBufferManager;
import py.common.tlsf.bytebuffer.manager.TlsfByteBufferManagerFactory;
import py.datanode.archive.ArchiveUnitBitMapAccessor;
import py.datanode.archive.RawArchive;
import py.datanode.configuration.DataNodeConfiguration;
import py.datanode.segment.datalog.SegmentLogMetadata;
import py.datanode.service.DataNodeRequestResponseHelper;
import py.engine.AbstractTask;
import py.engine.Result;
import py.engine.ResultImpl;
import py.engine.SingleTaskEngine;
import py.engine.Task;
import py.engine.TaskEngine;
import py.exception.StorageException;
import py.instance.Instance;
import py.instance.InstanceId;
import py.instance.InstanceStore;
import py.instance.PortType;
import py.membership.SegmentMembership;
import py.storage.impl.AsyncStorage;
import py.thrift.datanode.service.BroadcastRequest;
import py.thrift.datanode.service.DataNodeService;
import py.thrift.datanode.service.DataNodeService.AsyncClient;
import py.thrift.datanode.service.DataNodeService.AsyncClient.broadcast_call;
import py.thrift.datanode.service.DataNodeService.AsyncIface;
import py.token.controller.TokenController;
import py.token.controller.TokenControllerCenter;
import py.token.controller.TokenControllerUtils;

/**
 * use map(unique) and linked blocking queue(block to wait new task coming).
 *
 */
public class PersistDataToDiskEngineImpl implements PersistDataToDiskEngine {
  private static final Logger logger = LoggerFactory.getLogger(PersistDataToDiskEngineImpl.class);
  private static final int MAX_NUM_CONEXTS = 20000;
  private static Map<SegId, PersistDataContextWithPriority> segIdPersistDataContextWithPriorityMap;
  private static PersistDataToDiskEngineImpl instance = null;
  private static PriorityQueue persistContextPriorityQueue;
  private final TaskEngine getFlushTaskEngine;
  private boolean stopFlag;
  private ReentrantLock lock;
  private ThreadPoolExecutor threadPoolExecutor;
  // should set by outside at data node startup
  private InstanceStore instanceStore;
  private DataNodeConfiguration dataNodeCfg;
  private GenericThriftClientFactory<AsyncIface> dataNodeAsyncClientFactory;
  private SegmentUnitManager segmentUnitManager;

  private PersistDataToDiskEngineImpl() {
    this.segIdPersistDataContextWithPriorityMap = new ConcurrentHashMap<>();
    this.stopFlag = false;
    this.lock = new ReentrantLock();
    getFlushTaskEngine = new SingleTaskEngine();

    persistContextPriorityQueue = new PriorityQueue(new Comparator() {
      @Override
      public int compare(Object o1, Object o2) {
        int val1 = ((PersistDataContextWithPriority) o1).getPriority().get();
        int val2 = ((PersistDataContextWithPriority) o2).getPriority().get();
        if (val2 >= val1) {
          return 1;
        } else {
          return -1;
        }
      }
    });

    this.threadPoolExecutor = new ThreadPoolExecutor(1, 10, 60, TimeUnit.SECONDS,
        new SynchronousQueue<Runnable>(),
        new NamedThreadFactory("Persist-Data-Worker"));

  }

  public static void initInstance(DataNodeConfiguration dataNodeCfg, InstanceStore instanceStore,
      GenericThriftClientFactory<DataNodeService.AsyncIface> dataNodeAsyncClientFactory,
      SegmentUnitManager segmentUnitManager) {
    if (instance == null) {
      instance = new PersistDataToDiskEngineImpl();
      instance.dataNodeCfg = dataNodeCfg;
      instance.instanceStore = instanceStore;
      instance.dataNodeAsyncClientFactory = dataNodeAsyncClientFactory;
      instance.segmentUnitManager = segmentUnitManager;
      try {
        instance.start();
      } catch (Exception e) {
        logger.error("failed to start persist engine", e);
      }
    }
  }

  public static PersistDataToDiskEngineImpl getInstance() {
    Validate.notNull(instance, "should init instance at the beginning");
    return instance;
  }

  public static Map<SegId, PersistDataContextWithPriority> 
        getSegIdPersistDataContextWithPriorityMap() {
    return segIdPersistDataContextWithPriorityMap;
  }

  public static PriorityQueue getPersistContextPriorityQueue() {
    return persistContextPriorityQueue;
  }

  @Override
  public void asyncPersistData(PersistDataContext context) throws RejectedExecutionException {
    AbstractSegmentUnitMetadata data = context.getAbstractSegmentUnitMetadata();
    lock.lock();
    try {
      if (segIdPersistDataContextWithPriorityMap.size() >= MAX_NUM_CONEXTS) {
        throw new RejectedExecutionException(
            "too many context in the queue waiting to be processed");
      }

      PersistDataContextWithPriority contextWithPriority = segIdPersistDataContextWithPriorityMap
          .remove(data.getSegId());
      if (contextWithPriority == null) {
        contextWithPriority = new PersistDataContextWithPriority();
      } else {
        persistContextPriorityQueue.remove(contextWithPriority);
      }
      contextWithPriority.updataPersistContext(context);
      persistContextPriorityQueue.add(contextWithPriority);
      segIdPersistDataContextWithPriorityMap.put(data.getSegId(), contextWithPriority);
    } catch (RejectedExecutionException e) {
      throw e;
    } catch (Exception e) {
      logger.error("failed to put data:{} to persist engine", data, e);
      throw new RejectedExecutionException();
    } finally {
      lock.unlock();
    }
  }

  private void flushToDisk() {
    PersistDataContextWithPriority priorityContext = null;
    lock.lock();
    try {
      priorityContext = (PersistDataContextWithPriority) persistContextPriorityQueue.poll();
      if (priorityContext == null) {
        return;
      }
      segIdPersistDataContextWithPriorityMap.remove(priorityContext.getSegId());
    } finally {
      lock.unlock();
    }

    for (PersistDataContext persistContext : priorityContext.getAllContext()) {
      PersistWorker persistWorker = new PersistWorker(persistContext);
      try {
        threadPoolExecutor.execute(persistWorker);
        break;
      } catch (RejectedExecutionException e) {
        logger.debug("workers are busy at persisting data, waiting ...");
        try {
          Thread.sleep(100);
        } catch (InterruptedException e1) {
          logger.error("sleep has been InterruptedException");
        }
      }
    }

  }

  private void doPersistWork(AbstractSegmentUnitMetadata data, PersistDataType workType,
      RawArchive rawArchive)
      throws Exception {
    if (workType == PersistDataType.SegmentUnitMetadata) {
      SegmentUnitMetadataAccessor.writeSegmentUnitMetaToDisk((SegmentUnitMetadata) data);
      if (((SegmentUnitMetadata) data).getStatus() == SegmentUnitStatus.Deleted) {
        ((SegmentUnitMetadata) data).setIsPersistDeletedStatus(true);
        Validate.notNull(rawArchive);
        rawArchive.updateLogicalFreeSpaceToMetaData();
      }
    } else if (workType == PersistDataType.Bitmap) {
      SegId segId = data.getSegId();
      long plId = -1;
      SegmentUnit segmentUnit = segmentUnitManager.get(segId);
      if (segmentUnit == null) {
        logger.warn("can not get the segment unit when persisting bitmap for segId={}", segId);
      } else {
        SegmentLogMetadata segmentLogMetadata = segmentUnit.getSegmentLogMetadata();
        if (segmentLogMetadata != null) {
          plId = segmentLogMetadata.getPlId();
        }
      }

      ArchiveUnitBitMapAccessor.writeBitMapToDisk(data);
      data.getBrickMetadata().setBitmapNeedPersisted(false);

      if (plId != -1) {
        segmentUnit.getSegmentUnitMetadata().setLogIdOfPersistBitmap(plId);
      }
    } else if (workType == PersistDataType.ShadowUnitMetadata) {
      ShadowUnitMetadataAccessor.writeShadowUnitMetadataToDisk((ShadowUnitMetadata) data);
    } else if (workType == PersistDataType.BrickMetadata) {
      writeBrickMetadata(data.getBrickMetadata());
    } else if (workType == PersistDataType.SegmentUnitMetadataAndBrickMetadata) {
      writeBrickMetadata(data.getBrickMetadata());
      ShadowUnitMetadataAccessor.writeShadowUnitMetadataToDisk((ShadowUnitMetadata) data);
    } else if (workType == PersistDataType.SegmentUnitMetadataAndBitMap) {
      writeSegmentUnitMetadataAndBitMapToDisk((SegmentUnitMetadata) data);
      if (((SegmentUnitMetadata) data).getStatus() == SegmentUnitStatus.Deleted) {
        ((SegmentUnitMetadata) data).setIsPersistDeletedStatus(true);
        Validate.notNull(rawArchive);
        rawArchive.updateLogicalFreeSpaceToMetaData();
      }
    } else {
      throw new IllegalArgumentException(workType.toString());
    }
  }

  public void writeBrickMetadata(BrickMetadata brickMetadata,
      CompletionHandler<Integer, BrickMetadata> callback)
      throws JsonProcessingException, StorageException {
    logger.debug("write brick metadata and bitmap to disk: {}", brickMetadata);

    ObjectMapper mapper = new ObjectMapper();
    byte[] metadataBytes = mapper.writeValueAsBytes(brickMetadata);
    byte[] bitmapBytes = brickMetadata.bitmapByteArray();
    logger.debug("byte length: {} parsed results: {}", metadataBytes.length,
        new String(metadataBytes));
    Validate.isTrue(metadataBytes.length < ArchiveOptions.BRICK_METADATA_LENGTH);

    TlsfByteBufferManager tlsfByteBufferManager = TlsfByteBufferManagerFactory.instance();
    Validate.notNull(tlsfByteBufferManager);

    ByteBuffer buf = tlsfByteBufferManager
        .blockingAllocate((int) ArchiveOptions.BRICK_DESCDATA_LENGTH);

    try {
      buf.putLong(ArchiveOptions.BRICK_MAGIC).put(metadataBytes);
      buf.position(ArchiveOptions.BRICK_METADATA_LENGTH);
      buf.put(bitmapBytes);
      buf.clear();
      AsyncStorage asyncStorage = (AsyncStorage) brickMetadata.getStorage();
      asyncStorage.write(buf, brickMetadata.getMetadataOffsetInArchive(), brickMetadata,
          new CompletionHandler<Integer, BrickMetadata>() {
            @Override
            public void completed(Integer result, BrickMetadata attachment) {
              try {
                callback.completed(result, attachment);
              } finally {
                tlsfByteBufferManager.release(buf);
              }
            }

            @Override
            public void failed(Throwable exc, BrickMetadata attachment) {
              try {
                callback.failed(exc, attachment);
              } finally {
                tlsfByteBufferManager.release(buf);
              }
            }
          });
    } catch (StorageException e) {
      logger.error("storage exception", e);
      tlsfByteBufferManager.release(buf);
      throw e;
    }
  }

  public void writeBrickMetadata(BrickMetadata brickMetadata)
      throws JsonProcessingException, StorageException {
    logger.debug("write brick metadata and bitmap to disk: {}", brickMetadata);

    ObjectMapper mapper = new ObjectMapper();
    byte[] metadataBytes = mapper.writeValueAsBytes(brickMetadata);
    byte[] bitmapBytes = brickMetadata.bitmapByteArray();
    logger.debug("byte length: {} parsed results: {}", metadataBytes.length,
        new String(metadataBytes));
    Validate.isTrue(metadataBytes.length < ArchiveOptions.BRICK_METADATA_LENGTH);

    TlsfByteBufferManager tlsfByteBufferManager = TlsfByteBufferManagerFactory.instance();
    Validate.notNull(tlsfByteBufferManager);

    ByteBuffer buf = tlsfByteBufferManager
        .blockingAllocate((int) ArchiveOptions.BRICK_DESCDATA_LENGTH);

    byte z = 0;
    for (int i = 0; i < ArchiveOptions.BRICK_DESCDATA_LENGTH; i++) {
      buf.put(z);
    }
    buf.clear();
    try {
      buf.putLong(ArchiveOptions.BRICK_MAGIC).put(metadataBytes);
      buf.position(ArchiveOptions.BRICK_METADATA_LENGTH);
      buf.put(bitmapBytes);
      buf.clear();

      ObjectMapper mapper1 = new ObjectMapper();
      try {
        buf.getLong();
        int bytesLen = ArchiveOptions.BRICK_METADATA_LENGTH - Long.SIZE / Byte.SIZE;
        byte[] bytes = new byte[bytesLen];
        buf.get(bytes);
        BrickMetadata metadata = mapper1
            .readValue(bytes, 0, bytesLen, BrickMetadata.class);
        logger.warn("brick metadata {}", metadata);
      } catch (Exception e) {
        logger.warn("error ", e);
      }
      buf.clear();
      brickMetadata.getStorage().write(brickMetadata.getMetadataOffsetInArchive(), buf);
    } finally {
      tlsfByteBufferManager.release(buf);
    }
  }

  public void writeSegmentUnitMetadataAndBitMapAndBrickMetadataToDisk(SegmentUnit segmentUnit,
      CompletionHandler<Integer, SegmentUnitMetadata> callback)
      throws JsonProcessingException, StorageException {
    logger.debug("write segment unit metadata and bitmap and clean snapshot to disk: {}",
        segmentUnit);

    SegmentUnitMetadata unit = segmentUnit.getSegmentUnitMetadata();
    if (unit.getSegmentUnitType().equals(SegmentUnitType.Normal)) {
      BrickMetadata brickMetadata = unit.getBrickMetadata();

      CompletionHandler<Integer, BrickMetadata> completionHandler = 
          new CompletionHandler<Integer, BrickMetadata>() {
            @Override
            public void completed(Integer result, BrickMetadata attachment) {
              try {
                writeSegmentUnitMetadataAndBitMapToDisk(segmentUnit,
                    callback);
              } catch (Exception e) {
                logger.error(
                    "write segment unit metadata and bitmap and clean snapshot failed", e);
                callback.failed(e, unit);
              }
            }
    
            @Override
            public void failed(Throwable exc, BrickMetadata attachment) {
              callback.failed(exc, unit);
            }
          };

      this.writeBrickMetadata(brickMetadata, completionHandler);
    } else {
      writeSegmentUnitMetadataAndBitMapToDisk(segmentUnit,
          callback);
    }
  }

  public void writeSegmentUnitMetadataAndBitMapToDisk(SegmentUnit segmentUnit,
      CompletionHandler<Integer, SegmentUnitMetadata> callback)
      throws JsonProcessingException, StorageException {
    logger.debug("write segment unit metadata and bitmap and clean snapshot to disk: {}",
        segmentUnit);

    SegmentUnitMetadata unit = segmentUnit.getSegmentUnitMetadata();

    ObjectMapper mapper = new ObjectMapper();
    mapper.writeValueAsBytes(segmentUnit.getSegmentUnitMetadata());
    byte[] bytes = mapper.writeValueAsBytes(segmentUnit.getSegmentUnitMetadata());
    logger.debug("byte length: {} parsed results: {}", bytes.length, new String(bytes));
    Validate
        .isTrue(bytes.length + Long.SIZE / Byte.SIZE < ArchiveOptions.SEGMENTUNIT_METADATA_LENGTH,
            "segment unit meta data too long to persist, byte 's length is %d", bytes.length);

    SegId segId = segmentUnit.getSegId();
    long plId = -1;
    SegmentLogMetadata segmentLogMetadata = segmentUnit.getSegmentLogMetadata();
    if (segmentLogMetadata != null) {
      plId = segmentLogMetadata.getPlId();
      segmentUnit.getSegmentUnitMetadata().setLogIdOfPersistBitmap(plId);
      unit.setLogIdOfPersistBitmap(plId);
    }

    TlsfByteBufferManager tlsfByteBufferManager = TlsfByteBufferManagerFactory.instance();
    Validate.notNull(tlsfByteBufferManager);

    ByteBuffer buf;

    buf = tlsfByteBufferManager.blockingAllocate(
        ArchiveOptions.SEGMENTUNIT_METADATA_LENGTH + ArchiveOptions.SEGMENTUNIT_BITMAP_LENGTH);
    buf.clear();
    buf.putLong(ArchiveOptions.SEGMENT_UNIT_MAGIC);
    buf.put(bytes);
    buf.position(ArchiveOptions.SEGMENTUNIT_METADATA_LENGTH);
    byte[] bitMapBytes = unit.getBitmap().toByteArray();
    buf.put(bitMapBytes);
    buf.clear();

    AsyncStorage storage = (AsyncStorage) unit.getStorage();
    try {
      storage.write(buf, unit.getMetadataOffsetInArchive(), unit,
          new CompletionHandler<Integer, SegmentUnitMetadata>() {
            @Override
            public void completed(Integer result, SegmentUnitMetadata attachment) {
              try {
                callback.completed(result, attachment);
              } finally {
                tlsfByteBufferManager.release(buf);
              }
            }

            @Override
            public void failed(Throwable exc, SegmentUnitMetadata attachment) {
              try {
                callback.failed(exc, attachment);
              } finally {
                tlsfByteBufferManager.release(buf);
              }
            }
          });
    } catch (StorageException e) {
      logger.error("storage exception", e);
      tlsfByteBufferManager.release(buf);
      throw e;
    }
  }

  public void writeSegmentUnitMetadataAndBitMapToDisk(SegmentUnitMetadata unit) throws Exception {
    logger.debug("write segment unit metadata and bitmap and clean snapshot to disk: {}", unit);

    ObjectMapper mapper = new ObjectMapper();
    byte[] bytes = mapper.writeValueAsBytes(unit);
    logger.debug("byte length: {} parsed results: {}", bytes.length, new String(bytes));
    Validate
        .isTrue(bytes.length + Long.SIZE / Byte.SIZE < ArchiveOptions.SEGMENTUNIT_METADATA_LENGTH,
            "byte 's length is %d", bytes.length);

    SegId segId = unit.getSegId();
    long plId = -1;
    SegmentUnit segmentUnit = segmentUnitManager.get(segId);
    if (segmentUnit == null) {
      logger.warn("can not get the segment unit when persisting bitmap for segId={}", segId);
    } else {
      SegmentLogMetadata segmentLogMetadata = segmentUnit.getSegmentLogMetadata();
      if (segmentLogMetadata != null) {
        plId = segmentLogMetadata.getPlId();
        segmentUnit.getSegmentUnitMetadata().setLogIdOfPersistBitmap(plId);
        unit.setLogIdOfPersistBitmap(plId);
      }
    }

    TlsfByteBufferManager tlsfByteBufferManager = TlsfByteBufferManagerFactory.instance();
    Validate.notNull(tlsfByteBufferManager);

    ByteBuffer buf = tlsfByteBufferManager.blockingAllocate(
        ArchiveOptions.SEGMENTUNIT_METADATA_LENGTH + ArchiveOptions.SEGMENTUNIT_BITMAP_LENGTH);
    buf.clear();
    buf.putLong(ArchiveOptions.SEGMENT_UNIT_MAGIC);
    buf.put(bytes);
    buf.position(ArchiveOptions.SEGMENTUNIT_METADATA_LENGTH);
    byte[] bitMapBytes = unit.getBitmap().toByteArray();
    buf.put(bitMapBytes);
    buf.clear();
    try {
      unit.getStorage().write(unit.getMetadataOffsetInArchive(), buf);
    } finally {
      tlsfByteBufferManager.release(buf);
    }
  }

  private void broadcastNewMembershipToAllAliveSecondaries(SegId segId,
      SegmentMembership newMembership) {
    Set<InstanceId> aliveSecondaries = newMembership.getAliveSecondaries();
    if (aliveSecondaries.isEmpty()) {
      return;
    }

    BroadcastRequest request = DataNodeRequestResponseHelper
        .buildAddOrRemoveMemberRequest(segId, newMembership);
    EndPoint endPointToBroadcast = null;
    for (InstanceId secondaryId : aliveSecondaries) {
      try {
        Validate.notNull(instanceStore, "instanceStore can not be null");
        Instance hostToHearbeat = instanceStore.get(secondaryId);
        if (hostToHearbeat == null) {
          logger.warn("host:{} is probably down, just ignore it", secondaryId);
          continue;
        }

        endPointToBroadcast = hostToHearbeat.getEndPointByServiceName(PortType.HEARTBEAT);
        if (endPointToBroadcast != null) {
          DataNodeService.AsyncIface asyncClient = dataNodeAsyncClientFactory
              .generateAsyncClient(endPointToBroadcast);
          asyncClient.broadcast(request, new NoCareBroadcastMethodCallback());
          logger.debug("sent endpoint: {} a request {} ", endPointToBroadcast, request);
        }
      } catch (Exception e) {
        logger
            .warn("try to broadcast request:{} to endpoint:{} failed", request, endPointToBroadcast,
                e);
      }
    }
  }

  private void doContextWork(PersistDataContext context) {
    AbstractSegmentUnitMetadata data = context.getAbstractSegmentUnitMetadata();
    logger.debug("the data is {} ", data);
    SegId segId = data.getSegId();
    try {
      doPersistWork(data, context.getPersistWorkType(), context.getRawArchive());
    } catch (Exception e) {
      logger.error("failed to persist data:{} to disk", data, e);
      return;
    }

    if (context.isNeedBroadcastMembership()) {
      Validate.isTrue(data instanceof SegmentUnitMetadata);
      broadcastNewMembershipToAllAliveSecondaries(segId,
          ((SegmentUnitMetadata) data).getMembership());
    }
  }

  @Override
  public void start() throws Exception {
    TokenController controller = TokenControllerUtils
        .generateAndRegister(dataNodeCfg.getMaxPersistDataToDiskIoPs());
    TokenControllerCenter.getInstance().register(controller);
    getFlushTaskEngine.setTokenController(controller);
    getFlushTaskEngine.start();
    Task task = new AbstractTask() {
      @Override
      public Result work() {
        flushToDisk();
        if (!stopFlag) {
          getFlushTaskEngine.drive(this);
        }
        return ResultImpl.DEFAULT;
      }
    };
    stopFlag = false;
    getFlushTaskEngine.drive(task);
  }

  public TaskEngine getGetFlushTaskEngine() {
    return getFlushTaskEngine;
  }

  @Override
  public void stop() throws Exception {
    stopFlag = true;
    try {
      logger.warn("the task is {}", getFlushTaskEngine.getPendingTask());
      getFlushTaskEngine.stop();
      threadPoolExecutor.shutdown();
      threadPoolExecutor.awaitTermination(5, TimeUnit.MINUTES);
      segIdPersistDataContextWithPriorityMap.clear();
      persistContextPriorityQueue.clear();
      instance = null;
    } catch (Exception e) {
      logger.error("failed to exit persist engine", e);
    }
  }

  @Override
  public void syncPersistData(PersistDataContext context) {
    doContextWork(context);
  }

  public InstanceStore getInstanceStore() {
    return instanceStore;
  }

  public void setInstanceStore(InstanceStore instanceStore) {
    this.instanceStore = instanceStore;
  }

  public DataNodeConfiguration getDataNodeCfg() {
    return dataNodeCfg;
  }

  public PersistDataToDiskEngineImpl setDataNodeCfg(DataNodeConfiguration dataNodeCfg) {
    this.dataNodeCfg = dataNodeCfg;
    return this;
  }

  public GenericThriftClientFactory<DataNodeService.AsyncIface> getDataNodeAsyncClientFactory() {
    return dataNodeAsyncClientFactory;
  }

  public PersistDataToDiskEngineImpl setDataNodeAsyncClientFactory(
      GenericThriftClientFactory<DataNodeService.AsyncIface> dataNodeAsyncClientFactory) {
    this.dataNodeAsyncClientFactory = dataNodeAsyncClientFactory;
    return this;
  }

  public static class NoCareBroadcastMethodCallback implements
      AsyncMethodCallback<AsyncClient.broadcast_call> {
    @Override
    public void onComplete(broadcast_call arg0) {
    }

    @Override
    public void onError(Exception arg0) {
    }

  }

  private class PersistWorker implements Runnable {
    private PersistDataContext context;

    public PersistWorker(PersistDataContext context) {
      this.context = context;
    }

    @Override
    public void run() {
      doContextWork(context);
    }

  }
}
