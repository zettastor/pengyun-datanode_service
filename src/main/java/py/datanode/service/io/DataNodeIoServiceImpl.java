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

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.PbRequestResponseHelper;
import py.app.context.AppContext;
import py.archive.ArchiveOptions;
import py.archive.page.PageAddress;
import py.archive.segment.MigrationStatus;
import py.archive.segment.SegId;
import py.archive.segment.SegmentUnitMetadata;
import py.archive.segment.SegmentUnitStatus;
import py.archive.segment.SegmentUnitType;
import py.common.NamedThreadFactory;
import py.common.struct.EndPoint;
import py.common.struct.Pair;
import py.connection.pool.udp.detection.NetworkIoHealthChecker;
import py.consumer.AbstractMultiThreadConsumerService;
import py.consumer.ConsumerService;
import py.consumer.ConsumerServiceDispatcher;
import py.consumer.MultiThreadConsumerServiceWithBlockingQueue;
import py.consumer.SingleThreadConsumerService;
import py.datanode.archive.RawArchive;
import py.datanode.archive.RawArchiveManager;
import py.datanode.client.DataNodeServiceAsyncClientWrapper;
import py.datanode.configuration.DataNodeConfiguration;
import py.datanode.page.Page;
import py.datanode.page.PageManager;
import py.datanode.page.context.PageContextFactory;
import py.datanode.segment.SegmentUnit;
import py.datanode.segment.SegmentUnitManager;
import py.datanode.segment.copy.SecondaryCopyPageManager;
import py.datanode.segment.copy.task.SecondaryCopyPageTask;
import py.datanode.segment.datalog.LogIdGenerator;
import py.datanode.segment.datalog.LogImage;
import py.datanode.segment.datalog.MutationLogEntry;
import py.datanode.segment.datalog.SegmentLogMetadata;
import py.datanode.segment.datalog.broadcast.BrokenLogWithoutLogId;
import py.datanode.segment.datalog.broadcast.GiveYouLogIdEvent;
import py.datanode.segment.datalog.persist.full.TempLogPersister;
import py.datanode.segment.datalog.plal.engine.PlalEngine;
import py.datanode.segment.datalog.sync.log.SyncLogTaskExecutor;
import py.datanode.segment.datalog.sync.log.reduce.SyncLogReduceCollector;
import py.datanode.segment.datalog.sync.log.task.SyncLogPusher;
import py.datanode.segment.datalog.sync.log.task.SyncLogReceiver;
import py.datanode.segment.datalog.sync.log.task.SyncLogTask;
import py.datanode.service.DataNodeServiceImpl;
import py.datanode.service.Validator;
import py.datanode.service.io.throttle.IoThrottleManager;
import py.datanode.service.worker.MembershipPusher;
import py.datanode.statistic.AlarmReportData;
import py.datanode.statistic.AlarmReporter;
import py.datanode.statistic.AverageValueCollector;
import py.datanode.statistic.MaxValueCollector;
import py.instance.Instance;
import py.instance.InstanceId;
import py.instance.InstanceStore;
import py.instance.PortType;
import py.membership.SegmentMembership;
import py.metrics.PyMetric;
import py.metrics.PyMetricRegistry;
import py.metrics.PyTimerContext;
import py.metrics.PyTimerContextImpl;
import py.netty.client.GenericAsyncClientFactory;
import py.netty.core.AbstractMethodCallback;
import py.netty.core.IoEventThreadsMode;
import py.netty.core.MethodCallback;
import py.netty.core.TransferenceConfiguration;
import py.netty.datanode.AsyncDataNode;
import py.netty.datanode.NettyExceptionHelper;
import py.netty.datanode.PyCopyPageRequest;
import py.netty.datanode.PyReadResponse;
import py.netty.datanode.PyWriteRequest;
import py.netty.exception.AbstractNettyException;
import py.netty.exception.IncompleteGroupException;
import py.netty.exception.NetworkUnhealthyException;
import py.netty.exception.ServerPausedException;
import py.netty.exception.ServerShutdownException;
import py.netty.message.ProtocolBufProtocolFactory;
import py.netty.server.AsyncRequestHandler;
import py.netty.server.GenericAsyncServer;
import py.netty.server.GenericAsyncServerBuilder;
import py.netty.server.TransferenceServerOption;
import py.proto.Broadcastlog;
import py.proto.Broadcastlog.GiveYouLogIdRequest;
import py.proto.Broadcastlog.GiveYouLogIdResponse;
import py.proto.Broadcastlog.PbAsyncSyncLogBatchUnit;
import py.proto.Broadcastlog.PbAsyncSyncLogsBatchRequest;
import py.proto.Broadcastlog.PbAsyncSyncLogsBatchResponse;
import py.proto.Broadcastlog.PbBackwardSyncLogRequestUnit;
import py.proto.Broadcastlog.PbBackwardSyncLogResponseUnit;
import py.proto.Broadcastlog.PbBackwardSyncLogsRequest;
import py.proto.Broadcastlog.PbBackwardSyncLogsResponse;
import py.proto.Broadcastlog.PbBroadcastLog;
import py.proto.Broadcastlog.PbBroadcastLogManager;
import py.proto.Broadcastlog.PbCheckRequest;
import py.proto.Broadcastlog.PbCheckResponse;
import py.proto.Broadcastlog.PbCopyPageRequest;
import py.proto.Broadcastlog.PbCopyPageResponse;
import py.proto.Broadcastlog.PbCopyPageStatus;
import py.proto.Broadcastlog.PbErrorCode;
import py.proto.Broadcastlog.PbGetMembershipRequest;
import py.proto.Broadcastlog.PbGetMembershipResponse;
import py.proto.Broadcastlog.PbIoUnitResult;
import py.proto.Broadcastlog.PbReadRequest;
import py.proto.Broadcastlog.PbReadResponse;
import py.proto.Broadcastlog.PbWriteRequest;
import py.proto.Broadcastlog.PbWriteRequestUnit;
import py.proto.Broadcastlog.PbWriteResponse;
import py.proto.Broadcastlog.ReadCause;
import py.proto.Commitlog;
import py.proto.Commitlog.PbStartOnlineMigrationRequest;
import py.proto.Commitlog.PbStartOnlineMigrationResponse;
import py.proto.Commitlog.RequestType;
import py.storage.Storage;
import py.volume.VolumeType;

public class DataNodeIoServiceImpl implements AsyncDataNode.AsyncIface {
  private static final Logger logger = LoggerFactory.getLogger(DataNodeIoServiceImpl.class);
  private final PageContextFactory<Page> pageContextFactory;
 
 
  private final AbstractMultiThreadConsumerService<DelayedIoTask> copyEngine;
  private final ConsumerService<IoTask> readEngine;
  private final ConsumerService<WriteTask> writeEngine;
  private final ConsumerService<IoTask> giveYouLogIdEngine;
  private final ConsumerService<IoTask> checkEngine;

  // we only submit a request id (instead of a read task) to the delayed queue, because the tasks
  // are supposed to be executed after a long timeout (say 2 seconds), if we submit a whole read
  // task, we keep a reference to the read request and the buffer in it, thus the buffer can't be
  // released in time
  private final io.netty.util.Timer readTimeoutEngine;
  private final Multimap<Long, ReadCallbackTask> readRequestsMap = Multimaps
      .synchronizedListMultimap(LinkedListMultimap.create());
  private final DataNodeServiceImpl service;
  private final String className;
  private final AverageValueCollector averageIoDelayCollector = new AverageValueCollector();
  private final MaxValueCollector maxWriteQueueSizeCollector = new MaxValueCollector(0);
  private final MaxValueCollector maxGiveYouLogIdQueueSizeCollector = new MaxValueCollector(0);
  private final MaxValueCollector maxReadQueueSizeCollector = new MaxValueCollector(0);
  private final MaxValueCollector maxPendingRequestQueueSizeCollector = new MaxValueCollector(0);
  private final HashedWheelTimer giveLogIdHash;
  private SegmentUnitManager segmentUnitManager;
  private RawArchiveManager archiveManager;
  private DataNodeConfiguration cfg;
  private AppContext context;
  private PlalEngine plalEngine;
  private boolean shutdown = false;
  private boolean pauseRequestProcessing = false;
  private TempLogPersister tempLogPersister;
  private PageManager<Page> pageManager;
  private InstanceStore instanceStore;
  private DataNodeServiceAsyncClientWrapper dataNodeAsyncClient;
  private GenericAsyncServer genericAsyncServer;
  private ByteBufAllocator byteBufAllocator;
  private GenericAsyncClientFactory<AsyncDataNode.AsyncIface> asyncClientFactory;
  private IoThrottleManager ioThrottleManager;
  private AlarmReporter alarmReporter;
  private GenericAsyncServerBuilder serverFactory;

  // for sync log message reduce collector
  private SyncLogReduceCollector<PbBackwardSyncLogRequestUnit,
      PbBackwardSyncLogsRequest, PbBackwardSyncLogsRequest> backwardSyncLogRequestReduceCollector;
  private SyncLogReduceCollector<PbBackwardSyncLogResponseUnit,
      PbBackwardSyncLogsResponse, PbBackwardSyncLogsRequest> backwardSyncLogResponseReduceCollector;
  private SyncLogReduceCollector<PbAsyncSyncLogBatchUnit,
      PbAsyncSyncLogsBatchRequest, PbAsyncSyncLogsBatchRequest> syncLogBatchRequestReduceCollector;

  // for sync log task executor
  private SyncLogTaskExecutor syncLogTaskExecutor;

  // metric to measure read and write io delay
  private MetricRegistry metricRegistry;
  private Timer readDelayTimer;
  private Timer writeDelayTimer;
  private PyMetric timerCommitLogs;
  private PyMetric meterLogCompletedByCommitLog;
  private PyMetric meterLogCompletedByWriteCommitLog;
  private PyMetric waitingSyncLogRequestCount;
  private PyMetric waitingWriteLogRequestCount;
  private PyMetric waitingGiveLogIdRequestCount;
  private PyMetric waitingBackwardSyncLogRequestCount;
  private PyMetric waitingSyncLogRequestMeter;
  private PyMetric waitingBackwardSyncLogRequestMeter;
  private PyMetric meterReadData;
  private PyMetric timerReadData;
  private PyMetric timerGetPlansToApplyLogsForRead;
  private PyMetric timerMemoryCopyForRead;
  private PyMetric counterWorkingThreadForRead;
  private PyMetric meterNoEnoughThreadForRead;
  private PyMetric timerPrimaryWriteTaskProcessing;
  private PyMetric timerSecondaryWriteTaskProcessing;
  private PyMetric histoRequestUnitSizeForWrite;
  private PyMetric histoMergeCountForWrite;
  private PyMetric timerCreatePrimaryLogsForWrite;
  private PyMetric timerCreateSecondaryLogsForWrite;
  private PyMetric timerInsertPrimaryLogsForWrite;
  private PyMetric timerInsertSecondaryLogsForWrite;
  private PyMetric meterWriteData;
  private PyMetric timerWriteData;
  private PyMetric timerCommitLogsForWrite;
  private PyMetric counterWorkingThreadForWrite;
  private PyMetric counterWorkingThreadForSecondary;
  private PyMetric meterNoEnoughThreadForSecondary;
  private PyMetric meterNoEnoughThreadForPrimary;
  private PyMetric meterNotEnoughPrimaryBuffer;
  private PyMetric meterNotEnoughSecondaryBuffer;
  private PyMetric meterPrimaryFailToGetLock;
  private PyMetric meterSecondaryFailToGetLock;
  private PyMetric meterPrimaryCreateLogs;
  private PyMetric meterSecondaryCreateLogs;
  private PyMetric timerPrimaryGetLock;
  private PyMetric timerSecondaryGetLock;
  private PyMetric timerCopyBuffer;
  private PyMetric timerSaveShadowForCopyPage;
  private PyMetric counterWorkingThreadForCopyPage;
  private PyMetric meterNoEnoughThreadForCopyPage;
  private PyMetric timerGiveYouLogId;
  private PyMetric timerGiveYouLogIdWait;
  private PyMetric timerGiveYouLogIdProcess;
  private PyMetric counterOverallWriteQueueLength;
  private PyMetric counterOverallGiveYouLogIdQueueLength;
  private PyMetric counterOverallReadQueueLength;
  private PyMetric counterOverallTimeoutReadQueueLength;
  private PyMetric counterOverallCopyQueueLength;

  public DataNodeIoServiceImpl(DataNodeServiceImpl service) {
    this.className = getClass().getSimpleName();
    this.pageContextFactory = new PageContextFactory<>();
    this.service = service;
    initMetric();

    copyEngine = new MultiThreadConsumerServiceWithBlockingQueue<>(2, Runnable::run,
        new DelayQueue<>(), "io-service-copy");

    copyEngine.setCounter(n -> {
      counterOverallCopyQueueLength.incCounter(n);
    });

    readEngine = new ConsumerServiceDispatcher<RawArchive, IoTask>(
        ioTask -> ioTask.getSegmentUnit().getArchive(),
        rawArchive -> {
          SingleThreadConsumerService<IoTask> consumerService = new SingleThreadConsumerService<>(
              IoTask::run,
              new LinkedBlockingQueue<>(), "io-service-" + Storage
              .getDeviceName(rawArchive.getArchiveMetadata().getDeviceName() + "-read"));
          consumerService.setCounter(n -> {
            counterOverallReadQueueLength.incCounter(n);
            maxReadQueueSizeCollector.inc(n);
          });
          return consumerService;
        });

    this.readTimeoutEngine = new HashedWheelTimer(
        new NamedThreadFactory("io-service-time-out-read-handler"));

    checkEngine = new ConsumerServiceDispatcher<RawArchive, IoTask>(
        ioTask -> ioTask.getSegmentUnit().getArchive(),
        rawArchive -> new SingleThreadConsumerService<>(IoTask::run, new LinkedBlockingQueue<>(),
            "io-service-" + Storage
                .getDeviceName(
                    rawArchive.getArchiveMetadata().getDeviceName() + "-check-reachable")));

    giveYouLogIdEngine = new ConsumerServiceDispatcher<RawArchive, IoTask>(
        ioTask -> ioTask.getSegmentUnit().getArchive(), rawArchive -> {
      SingleThreadConsumerService<IoTask> consumerService = new SingleThreadConsumerService<>(
          IoTask::run,
          new LinkedBlockingQueue<>(), "io-service-" + Storage
          .getDeviceName(rawArchive.getArchiveMetadata().getDeviceName() + "-give-you-log-id"));
      consumerService.setCounter(n -> {
        counterOverallGiveYouLogIdQueueLength.incCounter(n);
        maxGiveYouLogIdQueueSizeCollector.inc(n);
      });
      return consumerService;
    });

    writeEngine = new ConsumerServiceDispatcher<RawArchive, WriteTask>(
        ioTask -> ioTask.getSegmentUnit().getArchive(), rawArchive -> {
      SingleThreadConsumerService<WriteTask> consumerService = new SingleThreadConsumerService<>(
          WriteTask::run,
          new PriorityBlockingQueue<>(),
          "io-service-" + Storage.getDeviceName(rawArchive.getArchiveMetadata().getDeviceName())
              + "-write-engine");
      consumerService.setCounter(n -> {
        counterOverallWriteQueueLength.incCounter(n);
        maxWriteQueueSizeCollector.inc(n);
      });
      return consumerService;
    });

    metricRegistry = new MetricRegistry();
    readDelayTimer = metricRegistry.timer("ioReadDelay");
    writeDelayTimer = metricRegistry.timer("ioWriteDelay");

    giveLogIdHash = new HashedWheelTimer(1, TimeUnit.SECONDS);
    giveLogIdHash.start();
  }

  public InstanceStore getInstanceStore() {
    return this.instanceStore;
  }

  public void setInstanceStore(InstanceStore instanceStore) {
    this.instanceStore = instanceStore;
  }

  public void setDataNodeAsyncClient(DataNodeServiceAsyncClientWrapper dataNodeAsyncClient) {
    this.dataNodeAsyncClient = dataNodeAsyncClient;
  }

  public SegmentUnitManager getSegmentUnitManager() {
    return segmentUnitManager;
  }

  public void setSegmentUnitManager(SegmentUnitManager segmentUnitManager) {
    this.segmentUnitManager = segmentUnitManager;
  }

  public RawArchiveManager getArchiveManager() {
    return archiveManager;
  }

  public void setArchiveManager(RawArchiveManager archiveManager) {
    this.archiveManager = archiveManager;
  }

  public DataNodeConfiguration getCfg() {
    return cfg;
  }

  public void setCfg(DataNodeConfiguration cfg) {
    this.cfg = cfg;
  }

  public AppContext getContext() {
    return context;
  }

  public void setContext(AppContext context) {
    this.context = context;
  }

  public PlalEngine getPlalEngine() {
    return plalEngine;
  }

  public void setPlalEngine(PlalEngine plalEngine) {
    this.plalEngine = plalEngine;
  }

  public TempLogPersister getTempLogPersister() {
    return tempLogPersister;
  }

  public void setTempLogPersister(TempLogPersister tempLogPersister) {
    this.tempLogPersister = tempLogPersister;
  }

  public PageManager<Page> getPageManager() {
    return pageManager;
  }

  public void setPageManager(PageManager<Page> pageManager) {
    this.pageManager = pageManager;
  }

  public void init() throws Exception {
    copyEngine.start();
    readEngine.start();
    writeEngine.start();
    giveYouLogIdEngine.start();
    checkEngine.start();

    PyMetricRegistry registry = PyMetricRegistry.getMetricRegistry();
    final PyMetric metricCounter = registry
        .register(MetricRegistry.name(AsyncRequestHandler.className, "counter_pending_requests"),
            Counter.class);

    TransferenceConfiguration transferenceConfiguration = GenericAsyncServerBuilder
        .defaultConfiguration();

    transferenceConfiguration.option(TransferenceServerOption.MAX_BYTES_ONCE_ALLOCATE,
        cfg.getNettyConfiguration().getNettyMaxBufLengthForAllocateAdapter());
    transferenceConfiguration.option(TransferenceServerOption.IO_EVENT_GROUP_THREADS_MODE,
        IoEventThreadsMode
            .findByValue(cfg.getNettyConfiguration().getNettyServerIoEventGroupThreadsMode()));
    transferenceConfiguration.option(TransferenceServerOption.IO_EVENT_GROUP_THREADS_PARAMETER,
        cfg.getNettyConfiguration().getNettyServerIoEventGroupThreadsParameter());
    transferenceConfiguration.option(TransferenceServerOption.IO_EVENT_HANDLE_THREADS_MODE,
        IoEventThreadsMode
            .findByValue(cfg.getNettyConfiguration().getNettyServerIoEventHandleThreadsMode()));
    transferenceConfiguration.option(TransferenceServerOption.IO_EVENT_HANDLE_THREADS_PARAMETER,
        cfg.getNettyConfiguration().getNettyServerIoEventHandleThreadsParameter());
    serverFactory = new GenericAsyncServerBuilder(this,
        ProtocolBufProtocolFactory.create(AsyncDataNode.AsyncIface.class),
        transferenceConfiguration);
    serverFactory.setMaxIoPendingRequests(cfg.getMaxIoPendingRequests());
    serverFactory.setAllocator(byteBufAllocator);
    serverFactory.setCounterPendingRequests(n -> {
      maxPendingRequestQueueSizeCollector.inc(n);
      metricCounter.incCounter(n);
    });

    EndPoint ioEndpoint = getContext().getEndPointByServiceName(PortType.IO);
    logger.warn("start listen port: {} for tcp server", ioEndpoint);
    genericAsyncServer = serverFactory.build(ioEndpoint);

    if (backwardSyncLogRequestReduceCollector != null) {
      
      backwardSyncLogRequestReduceCollector.setDataNodeAsyncClientFactory(asyncClientFactory);
      backwardSyncLogRequestReduceCollector.setDataNodeService(service);
      backwardSyncLogResponseReduceCollector.setDataNodeAsyncClientFactory(asyncClientFactory);
      backwardSyncLogResponseReduceCollector.setDataNodeService(service);
      syncLogBatchRequestReduceCollector.setDataNodeAsyncClientFactory(asyncClientFactory);
      syncLogBatchRequestReduceCollector.setDataNodeService(service);
      
    }

  }

  private int pageIndex(long pos) {
    return (int) (pos / (long) cfg.getPageSize());
  }

  public boolean belongToSamePage(long offsetInSeg1, long offsetInSeg2) {
    return (pageIndex(offsetInSeg1) == pageIndex(offsetInSeg2));
  }

  public void stopEngine() {
    if (writeEngine != null) {
      writeEngine.stop();
    }

    if (readEngine != null) {
      readEngine.stop();
    }

    if (readTimeoutEngine != null) {
      readTimeoutEngine.stop();
    }

    if (copyEngine != null) {
      copyEngine.stop();
    }

    if (checkEngine != null) {
      checkEngine.stop();
    }

    if (giveYouLogIdEngine != null) {
      giveYouLogIdEngine.stop();
    }

    if (giveLogIdHash != null) {
      giveLogIdHash.stop();
    }
    try {
      if (genericAsyncServer != null) {
        genericAsyncServer.shutdown();
      }

      if (serverFactory != null) {
        serverFactory.close();
      }
    } catch (Throwable t) {
      logger.error("caught an exception", t);
    }
  }

  public void shutdown() {
    logger.debug("io service impl has been shutdown {}", new Exception());
    shutdown = true;
  }

  public void initMetric() {
    PyMetricRegistry pyMetricRegistry = PyMetricRegistry.getMetricRegistry();
    timerReadData = pyMetricRegistry
        .register(MetricRegistry.name("DataNode", "Read", "timer_read_data"), Timer.class, true);
    timerGetPlansToApplyLogsForRead = pyMetricRegistry
        .register(MetricRegistry.name("DataNode", "Read", "timer_get_unapply_logs"), Timer.class);
    timerMemoryCopyForRead = pyMetricRegistry
        .register(MetricRegistry.name("DataNode", "Read", "timer_memory_copy_page"), Timer.class);

    meterReadData = pyMetricRegistry
        .register(MetricRegistry.name("DataNode", "Read", "meter_read_data"), Meter.class);
    counterWorkingThreadForRead = pyMetricRegistry
        .register(MetricRegistry.name("DataNode", "Read", "counter_working_thread"), Counter.class);
    meterNoEnoughThreadForRead = pyMetricRegistry
        .register(MetricRegistry.name("DataNode", "Read", "counter_no_enough_thread"), Meter.class);
    histoRequestUnitSizeForWrite = pyMetricRegistry
        .register(MetricRegistry.name("DataNode", "Write", "histo_request_unit_size"),
            Histogram.class);
    histoMergeCountForWrite = pyMetricRegistry
        .register(MetricRegistry.name("DataNode", "Write", "histo_merge_count"), Histogram.class);
    counterWorkingThreadForWrite = pyMetricRegistry
        .register(MetricRegistry.name("DataNode", "Write", "counter_working_thread"),
            Counter.class);
    counterWorkingThreadForSecondary = pyMetricRegistry
        .register(MetricRegistry.name("DataNode", "Write", "counter_working_thread_for_secondary"),
            Counter.class);
    meterNoEnoughThreadForSecondary = pyMetricRegistry
        .register(MetricRegistry.name("DataNode", "Write", "meter_no_enough_thread_for_secondary"),
            Meter.class);
    meterNoEnoughThreadForPrimary = pyMetricRegistry
        .register(MetricRegistry.name("DataNode", "Write", "meter_no_enough_thread_for_primary"),
            Meter.class);
    timerCreatePrimaryLogsForWrite = pyMetricRegistry
        .register(MetricRegistry.name("DataNode", "Write", "timer_create_logs_for_primary"),
            Timer.class);
    timerInsertPrimaryLogsForWrite = pyMetricRegistry
        .register(MetricRegistry.name("DataNode", "Write", "timer_insert_logs_for_primary"),
            Timer.class);
    timerCreateSecondaryLogsForWrite = pyMetricRegistry
        .register(MetricRegistry.name("DataNode", "Write", "timer_create_logs_for_secondary"),
            Timer.class);
    timerInsertSecondaryLogsForWrite = pyMetricRegistry
        .register(MetricRegistry.name("DataNode", "Write", "timer_insert_logs_for_secondary"),
            Timer.class);
    meterNotEnoughSecondaryBuffer = pyMetricRegistry
        .register(MetricRegistry.name("DataNode", "Write", "meter_not_enough_buffer_for_secondary"),
            Meter.class);
    meterPrimaryFailToGetLock = pyMetricRegistry
        .register(MetricRegistry.name("DataNode", "Write", "meter_primary_fail_to_get_lock"),
            Meter.class);
    meterSecondaryFailToGetLock = pyMetricRegistry
        .register(MetricRegistry.name("DataNode", "Write", "meter_secondary_fail_to_get_lock"),
            Meter.class);
    meterNotEnoughPrimaryBuffer = pyMetricRegistry
        .register(MetricRegistry.name("DataNode", "Write", "meter_not_enough_buffer_for_primary"),
            Meter.class);
    meterWriteData = pyMetricRegistry
        .register(MetricRegistry.name("DataNode", "Write", "meter_write_data"), Meter.class);
    timerWriteData = pyMetricRegistry
        .register(MetricRegistry.name("DataNode", "Write", "timer_write_data"), Timer.class, true);
    meterPrimaryCreateLogs = pyMetricRegistry
        .register(MetricRegistry.name("DataNode", "Write", "meter_primary_create_logs"),
            Meter.class);
    meterSecondaryCreateLogs = pyMetricRegistry
        .register(MetricRegistry.name("DataNode", "Write", "meter_secondary_create_logs"),
            Meter.class);
    timerPrimaryGetLock = pyMetricRegistry
        .register(MetricRegistry.name("DataNode", "Write", "timer_primary_get_lock"), Timer.class);
    timerSecondaryGetLock = pyMetricRegistry
        .register(MetricRegistry.name("DataNode", "Write", "timer_secondary_get_lock"),
            Timer.class);
    timerCopyBuffer = pyMetricRegistry
        .register(MetricRegistry.name("DataNode", "Write", "timer_copy_buffer"), Timer.class);
    timerCommitLogsForWrite = pyMetricRegistry
        .register(MetricRegistry.name("DataNode", "Write", "timer_commit_logs"), Timer.class);
    timerCommitLogs = pyMetricRegistry
        .register(MetricRegistry.name("DataNode", "commitlogs", "timer_seperate_commit_logs"),
            Timer.class);
    meterLogCompletedByCommitLog = pyMetricRegistry.register(
        MetricRegistry.name("DataNode", "counter_log_completed_by_commit_log"),
        Meter.class);
    meterLogCompletedByWriteCommitLog = pyMetricRegistry.register(MetricRegistry
            .name("DataNode", "counter_log_completed_by_commit_log_write"),
        Meter.class);
    timerSaveShadowForCopyPage = pyMetricRegistry
        .register(MetricRegistry.name("DataNode", "CopyPage", "timer_save_shadow_for_copy_page"),
            Timer.class);
    counterWorkingThreadForCopyPage = pyMetricRegistry
        .register(MetricRegistry.name("DataNode", "CopyPage", "counter_working_thread"),
            Counter.class);
    meterNoEnoughThreadForCopyPage = pyMetricRegistry
        .register(MetricRegistry.name("DataNode", "CopyPage", "meter_no_enough_thread"),
            Meter.class);

    timerGiveYouLogId = pyMetricRegistry
        .register(MetricRegistry.name("AsyncWrite", "Secondary", "timer_give_you_log_id"),
            Timer.class);
    timerGiveYouLogIdWait = pyMetricRegistry
        .register(MetricRegistry.name("AsyncWrite", "Secondary", "timer_give_you_log_id_wait"),
            Timer.class);
    timerGiveYouLogIdProcess = pyMetricRegistry
        .register(MetricRegistry.name("AsyncWrite", "Secondary", "timer_give_you_log_id_process"),
            Timer.class);

    counterOverallCopyQueueLength = pyMetricRegistry
        .register(MetricRegistry.name("DataNode", "AsyncCopy", "counter_overall_copy_queue_length"),
            Counter.class);
    counterOverallWriteQueueLength = pyMetricRegistry
        .register(
            MetricRegistry.name("DataNode", "AsyncWrite", "counter_overall_write_queue_length"),
            Counter.class);
    counterOverallGiveYouLogIdQueueLength = pyMetricRegistry
        .register(MetricRegistry
                .name("DataNode", "AsyncWrite", "counter_overall_give_you_log_id_queue_length"),
            Counter.class);
    counterOverallReadQueueLength = pyMetricRegistry
        .register(MetricRegistry.name("DataNode", "AsyncRead", "counter_overall_read_queue_length"),
            Counter.class);
    counterOverallTimeoutReadQueueLength = pyMetricRegistry
        .register(MetricRegistry
                .name("DataNode", "AsyncRead", "counter_overall_read_time_out_queue_length"),
            Counter.class);

    waitingSyncLogRequestCount = pyMetricRegistry
        .register(MetricRegistry.name("DataNode", "SyncLog", "counter_waiting_sync_log_request"),
            Counter.class);

    waitingWriteLogRequestCount = pyMetricRegistry
        .register(MetricRegistry.name("DataNode", "fq", "write_count"),
            Counter.class);

    waitingGiveLogIdRequestCount = pyMetricRegistry
        .register(MetricRegistry.name("DataNode", "fq", "counter_give_you_log_id_request"),
            Counter.class);
    waitingBackwardSyncLogRequestCount = pyMetricRegistry
        .register(
            MetricRegistry.name("DataNode", "SyncLog", "counter_waiting_backward_sync_log_request"),
            Counter.class);
    waitingSyncLogRequestMeter = pyMetricRegistry
        .register(MetricRegistry.name("DataNode", "SyncLog", "meter_waiting_sync_log_request"),
            Meter.class);
    waitingBackwardSyncLogRequestMeter = pyMetricRegistry
        .register(
            MetricRegistry.name("DataNode", "SyncLog", "meter_waiting_backward_sync_log_request"),
            Meter.class);
  }

  @Override
  public void ping(MethodCallback<Object> callback) {
  }

  private List<PbBroadcastLogManager> processLogsToCommit(List<Broadcastlog.PbBroadcastLogManager> 
      broadcastLogManagerList, SegmentUnit segUnit,
          boolean primary, boolean comeWithWrite) {
    SegmentLogMetadata segLogMetadata = segUnit.getSegmentLogMetadata();
    SegId segId = segUnit.getSegId();

    PyTimerContext commitLogsForWriteContext = timerCommitLogsForWrite.time();
    List<PbBroadcastLogManager> managersToBeCommited = new ArrayList<>();

    for (PbBroadcastLogManager manager : broadcastLogManagerList) {
      if (!primary) {
        Map hashMap = new HashMap();
        for (PbBroadcastLog broadcastLog : manager.getBroadcastLogsList()) {
          Validate.isTrue(broadcastLog.getLogId() != LogImage.INVALID_LOG_ID);
          hashMap.put(broadcastLog.getLogUuid(), broadcastLog.getLogId());
        }
        segLogMetadata.giveYouLogId(hashMap, plalEngine, false, comeWithWrite);
      }
      PbBroadcastLogManager.Builder builder = PbBroadcastLogManager.newBuilder();
      builder.setRequestId(manager.getRequestId());
      List<Pair<MutationLogEntry, PbBroadcastLog>> committedLogs = segLogMetadata
          .commitPbLogs(manager.getBroadcastLogsList(), primary);
     
      for (Pair<MutationLogEntry, PbBroadcastLog> pair : committedLogs) {
        if (pair.getFirst() != null) {
          plalEngine.putLog(segId, pair.getFirst());
          if (!primary) {
            if (comeWithWrite) {
              meterLogCompletedByWriteCommitLog.mark();
            } else {
              meterLogCompletedByCommitLog.mark();
            }
          }
        }
        builder.addBroadcastLogs(pair.getSecond());
      }

      managersToBeCommited.add(builder.build());
    }

    commitLogsForWriteContext.stop();
    return managersToBeCommited;
  }

  @Override
  public void discard(PbWriteRequest request, MethodCallback<PbWriteResponse> callback) {
    try {
      SegId segId = new SegId(request.getVolumeId(), request.getSegIndex());
      SegmentUnit segUnit = segmentUnitManager.get(segId);
      Long requestId = request.getRequestId();
      boolean zombieWrite = request.getZombieWrite();
      SegmentMembership requestMembership = PbRequestResponseHelper
          .buildMembershipFrom(request.getMembership());
      SegmentUnitStatus status;
      boolean isTempPrimary = false;
      boolean primary;
      try {
       
        primary = IoValidator.isPrimary(segId, requestMembership, context);
       
        if (zombieWrite) {
          Pair<SegmentUnitStatus, Boolean> statusAndIsTempPrimary = IoValidator
              .validateZombieWrite(segId, requestMembership, segUnit, context, primary,
                  cfg.isArbiterSyncPersistMembershipOnIoPath() && segUnit.isArbiter());
          status = statusAndIsTempPrimary.getFirst();
          isTempPrimary = statusAndIsTempPrimary.getSecond();
        } else if (primary) {
          boolean allowUnstablePrimary =
              request.hasUnstablePrimaryWrite() && request.getUnstablePrimaryWrite();
          status = IoValidator
              .validatePrimaryWriteRequest(segId, requestMembership, segUnit, context,
                  allowUnstablePrimary);
        } else {
          status = IoValidator
              .validateSecondaryWriteRequest(segId, requestMembership, segUnit, context);
        }
      } catch (AbstractNettyException e) {
        logger.info("membership validation failed, refuse write request {}", segId, e);
        callback.fail(e);
        return;
      }

      int i = 0;
      PbIoUnitResult[] unitResults = new PbIoUnitResult[request.getRequestUnitsCount()];
      for (PbWriteRequestUnit writeUnit : request.getRequestUnitsList()) {
        logger.debug("discard write request {}, unit: {}", request.getRequestId(), writeUnit);
        logger
            .debug("discard offset {} len {}, seg {}", writeUnit.getOffset(), writeUnit.getLength(),
                segId);

        long startOffset = writeUnit.getOffset();
        long totalLength = writeUnit.getLength();
        if (startOffset + totalLength > cfg.getSegmentUnitSize() || totalLength <= 0) {
          logger.error("wrong length {}", writeUnit);
          callback.fail(new Exception("wrong length"));
          return;
        }

        unitResults[i] = PbIoUnitResult.SKIP;

        long consumedLength = cfg.getPageSize();
        for (long offset = startOffset; offset < (startOffset + totalLength);
            offset += consumedLength) {
          long remainder = offset % cfg.getPageSize();
          if (remainder > 0) {
            logger.info("startoffset has remainder {}", startOffset);
            consumedLength = cfg.getPageSize() - remainder;
          }
          logger
              .debug("discard slice, offset {}, length {} for writeUnit {}", offset, consumedLength,
                  writeUnit);

          int pageIndex = (int) (offset / cfg.getPageSize());
          logger.debug("going to free page {} for seg {}", pageIndex, segId);
          segUnit.freePage(pageIndex);

          PageAddress pageAddress = segUnit.getLogicalPageAddressToApplyLog(pageIndex);
        }

        unitResults[i] =
            (primary || isTempPrimary) ? PbIoUnitResult.PRIMARY_COMMITTED : PbIoUnitResult.OK;
        i++;
      }

      PbWriteResponse.Builder responseBuilder = PbWriteResponse.newBuilder();
      responseBuilder.setRequestId(requestId);
      i = 0;
      for (PbWriteRequestUnit writeUnit : request.getRequestUnitsList()) {
        if (primary || isTempPrimary) {
          responseBuilder.addResponseUnits(PbRequestResponseHelper
              .buildPbWriteResponseUnitFrom(writeUnit, unitResults[i++], LogImage.DISCARD_LOG_ID));
        } else {
          responseBuilder.addResponseUnits(
              PbRequestResponseHelper.buildPbWriteResponseUnitFrom(writeUnit, unitResults[i++]));
        }

      }
      callback.complete(responseBuilder.build());
    } catch (Exception e) {
      logger.error("caught an exception when process discard request", e);
      callback.fail(e);
    }
  }

  @Override
  public void startOnlineMigration(PbStartOnlineMigrationRequest request,
      MethodCallback<PbStartOnlineMigrationResponse> callback) {
    long volumeId = request.getVolumeId();
    long requestId = request.getRequestId();

    if (shutdown) {
      logger.warn("datanode shutting down, refuse startOnlineMigration request:{} volumeId:{}",
          requestId, volumeId);
      callback.fail(new ServerShutdownException("my instance: " + context.getInstanceId()));
      return;
    }

    if (pauseRequestProcessing) {
      logger.warn("datanode paused, refuse startOnlineMigration request:{} volumeId:{} ", requestId,
          volumeId);
      callback.fail(new NetworkUnhealthyException("refuse write"));
      return;
    }

    PbStartOnlineMigrationResponse.Builder builder = PbStartOnlineMigrationResponse.newBuilder();
    builder.setRequestId(requestId);
    builder.setInstanceId(context.getInstanceId().getId());
    Collection<SegmentUnit> segmentUnits = segmentUnitManager
        .get(segmentUnit -> segmentUnit.getSegId().getVolumeId().getId() == request.getVolumeId());

    Iterator<SegmentUnit> it = segmentUnits.iterator();
    while (it.hasNext()) {
      SegmentUnit segUnit = it.next();
      if (segUnit.getSegmentUnitMetadata().isArbiter()) {
        continue;
      }

      if (!segUnit.getSegmentUnitMetadata().isOnlineMigrationSegmentUnit()) {
        logger.warn("segUnit:{} is not online migration.", segUnit.getSegId());
        callback.fail(NettyExceptionHelper.buildServerProcessException("not migration"));
      }

      logger.warn("reset SegId {} status to START.", segUnit.getSegId());
    }

    callback.complete(builder.build());
  }

  @Override
  public void write(PyWriteRequest writeRequest, MethodCallback<PbWriteResponse> methodCallback) {
    PbWriteRequest request = writeRequest.getMetadata();
    boolean zombieWrite = request.getZombieWrite();
    SegId segId = new SegId(request.getVolumeId(), request.getSegIndex());

    if (cfg.isEnableIoTracing()) {
      logger.warn("{} received write request: {} for segId: {}", context.getInstanceId(),
          request.getRequestId(), segId);
    }

    logger.debug("{} received write request: {} for segId: {}", context.getInstanceId(),
        request.getRequestId(),
        segId);
    PyTimerContext writeContext = getWriteDelayTimer();
    meterWriteData.mark();
    waitingWriteLogRequestCount.incCounter();
    MethodCallback<PbWriteResponse> callback = new MethodCallback<PbWriteResponse>() {
      @Override
      public void complete(PbWriteResponse object) {
        waitingWriteLogRequestCount.decCounter();
        if (cfg.isEnableIoTracing()) {
          logger.warn("done request {}", request.getRequestId());
        }
        logger.debug("done request {}", request.getRequestId());
        methodCallback.complete(object);
        if (cfg.isEnableIoTracing()) {
          logger.warn("responded request {}", request.getRequestId());
        }
        long writeTimeDelay = writeContext.stop();
        reportWriteTimeDelay(writeTimeDelay, request.getRequestId(), segId);
      }

      @Override
      public void fail(Exception e) {
        waitingWriteLogRequestCount.decCounter();
        if (cfg.isEnableIoTracing()) {
          logger.warn("failed request {}", request.getRequestId(), e);
        }
        logger.debug("failed request {}", request.getRequestId());
        methodCallback.fail(e);
        writeContext.stop();
      }

      @Override
      public ByteBufAllocator getAllocator() {
        return methodCallback.getAllocator();
      }
    };

    Long requestId = request.getRequestId();
    int step = 0;

    SegmentMembership requestMembership = PbRequestResponseHelper
        .buildMembershipFrom(request.getMembership());
    if (shutdown) {
      logger.info("datanode shutting down, refuse write request {}", segId);
      callback.fail(new ServerShutdownException("my instance: " + context.getInstanceId()));
      return;
    }

    if (pauseRequestProcessing) {
      logger.error("network unhealthy, refuse write !");
      callback.fail(new ServerPausedException("network unhealthy, refuse write !"));
      return;
    }

    SegmentUnit segUnit = segmentUnitManager.get(segId);
    SegmentUnitStatus status;
    boolean isTempPrimary = false;
    boolean primary;
    try {
     
      primary = IoValidator.isPrimary(segId, requestMembership, context);
     
      if (zombieWrite) {
        Pair<SegmentUnitStatus, Boolean> statusAndIsTempPrimary = IoValidator
            .validateZombieWrite(segId, requestMembership, segUnit, context, primary,
                cfg.isArbiterSyncPersistMembershipOnIoPath() && segUnit.isArbiter());
        status = statusAndIsTempPrimary.getFirst();
        isTempPrimary = statusAndIsTempPrimary.getSecond();
      } else if (primary) {
        boolean allowUnstablePrimary =
            request.hasUnstablePrimaryWrite() && request.getUnstablePrimaryWrite();
        status = IoValidator
            .validatePrimaryWriteRequest(segId, requestMembership, segUnit, context,
                allowUnstablePrimary);
      } else {
        status = IoValidator
            .validateSecondaryWriteRequest(segId, requestMembership, segUnit, context);
      }

    } catch (AbstractNettyException e) {
      logger.info("membership validation failed, refuse write request {}", segId, e);
      callback.fail(e);
      return;
    }

    SegmentLogMetadata logMetadata = segUnit.getSegmentLogMetadata();

    PbWriteResponse.Builder responseBuilder = PbWriteResponse.newBuilder();
    responseBuilder.setRequestId(request.getRequestId());
    responseBuilder.addAllLogManagersToCommit(
        processLogsToCommit(request.getBroadcastManagersList(), segUnit, status.isPrimary(), true));

    SegmentMembership currentMembership = segUnit.getSegmentUnitMetadata().getMembership();
    if (status.isPrimary() || isTempPrimary) {
      if (!currentMembership.equals(requestMembership)) {
        responseBuilder
            .setMembership(PbRequestResponseHelper.buildPbMembershipFrom(currentMembership));
      }
    }

    int i = 0;
    PbIoUnitResult[] unitResults = new PbIoUnitResult[request.getRequestUnitsCount()];
    int logsToProcess = 0;
    for (PbWriteRequestUnit writeUnit : request.getRequestUnitsList()) {
      int pageIndex = (int) (writeUnit.getOffset() / cfg.getPageSize());
      logger.debug("write request {}, unit: {}", request.getRequestId(), writeUnit);
      logger.debug("write offset {} len {} pageIndex {}, seg {}, page size {}, is free page {}",
          writeUnit.getOffset(), writeUnit.getLength(), pageIndex, segId, cfg.getPageSize(),
          segUnit.getSegmentUnitMetadata().isPageFree(pageIndex));
      histoRequestUnitSizeForWrite.updateHistogram(writeUnit.getLength());

      PbIoUnitResult result = IoValidator
          .validateWriteInput(writeUnit.getLength(), writeUnit.getOffset(), cfg.getPageSize(),
              cfg.getSegmentUnitSize());
      if (result != PbIoUnitResult.OK) {
        unitResults[i++] = result;
        logMetadata
            .markLogBroken(writeUnit.getLogUuid(), BrokenLogWithoutLogId.BrokenReason.BadRequest);
        logger.error("invalid input write unit: {}", writeUnit);
        continue;
      }

      if (writeUnit.getLogId() != LogImage.INVALID_LOG_ID
          && logMetadata.getLog(writeUnit.getLogId()) != null) {
        logger.warn("the log has exist, for logId: {}", writeUnit.getLogId());
        result = status.isStable() ? PbIoUnitResult.OK : PbIoUnitResult.SECONDARY_NOT_STABLE;
        unitResults[i++] = result;
        continue;
      }

      unitResults[i++] = PbIoUnitResult.SKIP;
      logsToProcess++;

    }

    if (logsToProcess == 0) {
      i = 0;
      for (PbWriteRequestUnit writeUnit : request.getRequestUnitsList()) {
        responseBuilder.addResponseUnits(
            PbRequestResponseHelper.buildPbWriteResponseUnitFrom(writeUnit, unitResults[i++]));
      }
      callback.complete(responseBuilder.build());
    } else {
      WriteTask task;
      if (primary || isTempPrimary) {
        task = new PrimaryWriteTask(writeRequest, callback, responseBuilder, unitResults, segUnit,
            plalEngine, asyncClientFactory, instanceStore, cfg, isTempPrimary,
            context.getInstanceId(),
            cfg.isInfiniteTimeoutAllocatingFastBuffer());
      } else {
        task = new SecondaryWriteTask(writeRequest, callback, responseBuilder, unitResults,
            segUnit,
            plalEngine, status.isStable(), cfg, cfg.isInfiniteTimeoutAllocatingFastBuffer());
      }
     
      writeEngine.submit(task);
     
    }
    ioThrottleManager.markNormalIoComes(segUnit.getArchive(), true);
  }

  private void processLogsToCommitWhenRead(SegmentUnit segmentUnit, PbReadRequest request) {
    SegmentLogMetadata segmentLogMetadata = segmentUnit.getSegmentLogMetadata();
    for (Long logId : request.getLogsToCommitList()) {
      MutationLogEntry entry = segmentLogMetadata.getLog(logId);
      logger.info("commit logs for read: {}", entry);
      if (entry != null && !entry.isFinalStatus()) {
       
        segmentLogMetadata.commitLog(logId);
        entry.commit();
        if (entry.isCommitted()) {
          int pageIndex = (int) (entry.getOffset() / ArchiveOptions.PAGE_SIZE);
          segmentUnit.setPageHasBeenWrittenAndFirstWritePageIndex(pageIndex);

        }
        if (!entry.isApplied()) {
          plalEngine.putLog(segmentLogMetadata.getSegId(), entry);
        }
      }
    }
  }

  @Override
  public void giveYouLogId(GiveYouLogIdRequest request,
      MethodCallback<GiveYouLogIdResponse> callback) {
    logger.debug("give you log id {} {} seg {} {}", request.getRequestId(),
        request.getLogUuidAndLogIdsList(), request.getVolumeId(), request.getSegIndex());

    if (shutdown) {
      callback.fail(new ServerShutdownException("my instance: " + context.getInstanceId()));
      return;
    }
    if (pauseRequestProcessing) {
      callback.fail(new NetworkUnhealthyException());
      return;
    }

    SegId segId = new SegId(request.getVolumeId(), request.getSegIndex());
    SegmentUnit segmentUnit = segmentUnitManager.get(segId);
    try {
      IoValidator.validateGiveYouLogIdRequest(segId, segmentUnit, context, request,
          cfg.isArbiterSyncPersistMembershipOnIoPath() && segmentUnit.isArbiter());
    } catch (AbstractNettyException e) {
      callback.fail(e);
      return;
    }

    if (segmentUnit.isArbiter()) {
      logger.info("arbiter complete giveyoulogid request {} {}", request.getRequestId(), segId);
      callback.complete(GiveYouLogIdResponse.newBuilder().setRequestId(request.getRequestId())
          .setMyInstanceId(context.getInstanceId().getId()).build());
      return;
    }

    GiveYouLogIdTask task = new GiveYouLogIdTask(segmentUnit, request, callback);
    giveYouLogIdEngine.submit(task);
  }

  @Override
  public void getMembership(PbGetMembershipRequest request,
      MethodCallback<PbGetMembershipResponse> callback) {
    if (shutdown) {
      callback.fail(new ServerShutdownException("my instance: " + context.getInstanceId()));
      return;
    }
    if (pauseRequestProcessing) {
      callback.fail(new NetworkUnhealthyException());
      return;
    }
    SegId segId = new SegId(request.getVolumeId(), request.getSegIndex());
    logger.debug("get membership request:{}, segId:{}", request, segId);
    SegmentUnit segUnit = segmentUnitManager.get(segId);
    if (segUnit == null) {
      callback.fail(NettyExceptionHelper.buildSegmentNotFoundException(segId, context));
      return;
    } else {
      SegmentMembership membership = segUnit.getSegmentUnitMetadata().getMembership();
      PbGetMembershipResponse.Builder builder = PbGetMembershipResponse.newBuilder();
      builder.setPbMembership(PbRequestResponseHelper.buildPbMembershipFrom(membership));
      builder.setRequestId(request.getRequestId());
      callback.complete(builder.build());
      return;
    }
  }

  @Override
  public void addOrCommitLogs(Commitlog.PbCommitlogRequest request,
      MethodCallback<Commitlog.PbCommitlogResponse> callback) {
    if (shutdown) {
      callback.fail(new ServerShutdownException(
          "myself : " + context.getInstanceId() + " request id" + request.getRequestId()));
      return;
    }
    if (pauseRequestProcessing) {
      callback.fail(new NetworkUnhealthyException());
      return;
    }

    SegmentMembership requestMembership = PbRequestResponseHelper
        .buildMembershipFrom(request.getMembership());
    SegId segId = new SegId(request.getVolumeId(), request.getSegIndex());

    SegmentUnit segUnit = segmentUnitManager.get(segId);
    if (segUnit == null) {
      logger.error("segment not found for segId={} request id={}", segId, request.getRequestId());
      callback.fail(NettyExceptionHelper.buildSegmentNotFoundException(segId, context));
      return;
    }

    SegmentMembership currentMembership = segUnit.getSegmentUnitMetadata().getMembership();

    boolean isPrimary;
    InstanceId currentInstanceId = context.getInstanceId();
    if (requestMembership.getPrimary().equals(currentInstanceId)) {
      isPrimary = true;
    } else {
      if (requestMembership.getSecondaries().contains(currentInstanceId) || requestMembership
          .getJoiningSecondaries().contains(currentInstanceId)) {
        isPrimary = false;
      } else {
       
        logger
            .warn("requestMembership{} currentMembership:{}", requestMembership, currentMembership);
        callback.fail(
            NettyExceptionHelper.buildMembershipVersionLowerException(segId, requestMembership));
        return;
      }
    }

    if (request.getType() == RequestType.COMMIT) {
      Commitlog.PbCommitlogResponse.Builder builder = Commitlog.PbCommitlogResponse
          .newBuilder();
      List<PbBroadcastLogManager> commitLogManagers = processLogsToCommit(
          request.getBroadcastManagersList(), segUnit, isPrimary, false);
      builder.addAllLogManagersToCommit(commitLogManagers);

      builder.setEndPoint(context.getMainEndPoint().toString());
      builder.setRequestId(request.getRequestId());
      builder.setVolumeId(request.getVolumeId());
      builder.setMembership(PbRequestResponseHelper.buildPbMembershipFrom(currentMembership));
      builder.setSuccess(true);
      builder.setSegIndex(request.getSegIndex());
      callback.complete(builder.build());
    } else if (request.getType() == RequestType.ADD) {
      AddOrCommitLogsTask task = new AddOrCommitLogsTask(segUnit, request, callback,
          plalEngine, cfg.getPageSize());
      giveYouLogIdEngine.submit(task);
    } else {
      logger.error("unknown type {}", request.getType());
      callback.fail(NettyExceptionHelper
          .buildServerProcessException("unknown type " + request.getType()));
    }
  }

  @Override
  public void read(PbReadRequest request, MethodCallback<PyReadResponse> methodCallback) {
    final PyTimerContext readContext = getReadDelayTimer();
    meterReadData.mark();

    SegId segId = new SegId(request.getVolumeId(), request.getSegIndex());
    if (cfg.isEnableIoTracing()) {
      logger.warn("read request:{}, segId:{}", request, segId);
    }
    logger.debug("read request:{}, segId:{}", request, segId);
    long uniqueRequestId = ReadScheduleTask.uniqueReadRequestId();
    MethodCallback<PyReadResponse> callback = new MethodCallback<PyReadResponse>() {
      @Override
      public void complete(PyReadResponse object) {
        readRequestsMap.removeAll(uniqueRequestId);
        if (cfg.isEnableIoTracing()) {
          logger.warn("done read request:{}, segId:{}", request, segId);
        }

        methodCallback.complete(object);
        if (cfg.isEnableIoTracing()) {
          logger.warn("responded read request:{}, segId:{}", request, segId);
        }
        long readDelay = readContext.stop();
        if (getAlarmReporter() != null) {
         
          reportReadTimeDelay(readDelay, request.getRequestId(), segId);
        }
      }

      @Override
      public void fail(Exception e) {
        readRequestsMap.removeAll(uniqueRequestId);
        if (cfg.isEnableIoTracing()) {
          logger.warn("failed read request:{}, segId:{}", request, segId, e);
        }
        methodCallback.fail(e);
        readContext.stop();
      }

      @Override
      public ByteBufAllocator getAllocator() {
        return methodCallback.getAllocator();
      }
    };

    if (shutdown) {
      callback.fail(new ServerShutdownException("my instance: " + context.getInstanceId()));
      return;
    }

    if (pauseRequestProcessing) {
      logger.error("network unhealthy, refuse read!");
      callback.fail(new ServerPausedException("network unhealthy, refuse read!"));
      return;
    }

    SegmentMembership other = PbRequestResponseHelper.buildMembershipFrom(request.getMembership());
    SegmentUnit segUnit = segmentUnitManager.get(segId);

    boolean isPrimary;
    ReadCause readCause = request.getReadCause();
    try {
      isPrimary = IoValidator.isPrimary(segId, other, context);
      IoValidator.validateReadRequest(segId, segUnit, context, other, isPrimary, readCause);
    } catch (AbstractNettyException e) {
      callback.fail(e);
      return;
    }

    Long pcl = null;
    if (readCause == ReadCause.CHECK) {
      PbReadResponse.Builder responseBuilder = PbReadResponse.newBuilder();
      responseBuilder.setRequestId(request.getRequestId());
      callback.complete(new PyReadResponse(responseBuilder.build(), null, false));
      return;
    }

    processLogsToCommitWhenRead(segUnit, request);

    boolean mergeRead = false;

    if (request.getReadCause() == ReadCause.FETCH) {
      if (segUnit.getSegmentUnitMetadata().getStatus() != SegmentUnitStatus.Primary
          && !segUnit.isAllowReadDataEvenNotPrimary()) {
        logger.warn("not primary, merge reading {} {}", segUnit.getSegId(),
            request.getRequestId());
        mergeRead = true;
      }
    }

    if (segUnit.isAllowReadDataEvenNotPrimary()) {
      mergeRead = false;
    }
    if (mergeRead) {
      SegmentMembership membership = segUnit.getSegmentUnitMetadata().getMembership();
      CompletableFuture<PyReadResponse> future = MergeReadAlgorithmBuilder
          .build(instanceStore, context.getInstanceId(), segUnit,
              segUnit.getSegmentUnitMetadata().getMembership(), cfg.getPageSize(), request,
              getAsyncClientFactory(), getByteBufAllocator()).process();
      future
          .thenAccept(resp -> {
            SegmentMembership latestMembership = segUnit.getSegmentUnitMetadata()
                .getMembership();
            if (!latestMembership.equals(membership)) {
              logger.warn("membership has been changed just now!");
             
              resp.release();
              callback.fail(NettyExceptionHelper
                  .buildMembershipVersionLowerException(segId, latestMembership));
            } else {
              callback.complete(resp);
            }
          })
          .exceptionally(e -> {
            logger.warn("caught a throwable reading for {}", segId, e);
            callback.fail(NettyExceptionHelper.buildServerProcessException(e));
            return null;
          });
    } else {
      new ReadScheduleTask(cfg, readRequestsMap, callback, segUnit,
          pageManager, service, readEngine, readTimeoutEngine, pageContextFactory, request,
          uniqueRequestId)
          .run();
    }

    ioThrottleManager.markNormalIoComes(segUnit.getArchive(), false);
  }

  @Override
  public void copy(PyCopyPageRequest pyCopyPageRequest,
      MethodCallback<PbCopyPageResponse> callback) {
   
   
    PbCopyPageRequest request = pyCopyPageRequest.getMetadata();
    logger.debug("copy {}", request.getRequestId());
    SegId segId = new SegId(request.getVolumeId(), request.getSegIndex());
    SegmentUnit segmentUnit = segmentUnitManager.get(segId);

    logger.info(
        "PbCopyPageRequest, receive copy page request,"
            + " requestId: {},  sessionId: {}, pageCount: {}, unit index: {} for segId: {}",
        request.getRequestId(), request.getSessionId(), request.getPageRequestsList().size(),
        request.getCopyPageUnitIndex(), request.getTaskId(), segId);
    boolean abortCopy = false;
    SecondaryCopyPageManager secondaryCopyPageManager = segmentUnit.getSecondaryCopyPageManager();
    if (secondaryCopyPageManager == null) {
      abortCopy = true;
    }
    if (shutdown || pauseRequestProcessing) {
      logger.warn("system is being shutdown or request processing paused");
      abortCopy = true;
    }

    SegmentMembership requestMembership = PbRequestResponseHelper
        .buildMembershipFrom(request.getMembership());
    if (request.getStatus() == PbCopyPageStatus.COPY_PAGE_ABORT) {
      logger.error("primary has told me that aborts the copy-page immediately: {}", segId);
      abortCopy = true;
    }

    try {
      Validator.validateMigrationPagesInput(segmentUnit, requestMembership, context);
    } catch (Exception e) {
      logger.error("membership has changed for segId: {}", segId, e);
      abortCopy = true;
    }

    PbCopyPageResponse.Builder responseBuilder = PbCopyPageResponse.newBuilder();
    responseBuilder.setRequestId(request.getRequestId());
    responseBuilder.setSessionId(request.getSessionId());
    responseBuilder.setCopyPageUnitIndex(request.getCopyPageUnitIndex());
    responseBuilder.addAllNexLogUnits(new ArrayList<>());
    if (abortCopy) {
      responseBuilder.setStatus(PbCopyPageStatus.COPY_PAGE_ABORT);
      callback.complete(responseBuilder.build());
      return;
    }
    responseBuilder.setStatus(PbCopyPageStatus.COPY_PAGE_PROCESSING);

    SecondaryCopyPageTask copyPageTask = new SecondaryCopyPageTask(0, pyCopyPageRequest, callback,
        responseBuilder, segmentUnit, pageManager, copyEngine);
    logger.info("submit copy task {} unit {}", copyPageTask, request.getCopyPageUnitIndex());
    copyEngine.submit(copyPageTask);
  }

  @Override
  public void backwardSyncLog(PbBackwardSyncLogsRequest request,
      MethodCallback<PbBackwardSyncLogsResponse> callback) {
    logger.debug("got a backward sync log request {}", request);
    if (shutdown) {
      callback.fail(new ServerShutdownException(
          "myself : " + context.getInstanceId() + " request option" + request));
      return;
    }
    if (pauseRequestProcessing) {
      callback.fail(new NetworkUnhealthyException());
      return;
    }

    waitingBackwardSyncLogRequestMeter.mark();
    waitingBackwardSyncLogRequestCount.incCounter();
    long requestId = request.getRequestId();
    MethodCallback<PbBackwardSyncLogsResponse> methodCallback =
        new MethodCallback<PbBackwardSyncLogsResponse>() {
          @Override
          public void complete(PbBackwardSyncLogsResponse object) {
            waitingBackwardSyncLogRequestCount.decCounter();
            logger.info("return a backward sync log request {}", requestId);
            callback.complete(object);
          }

          @Override
          public void fail(Exception e) {
            waitingBackwardSyncLogRequestCount.decCounter();
            logger.warn("return failed a backward sync log request {}", requestId);
            callback.fail(e);
          }

          @Override
          public ByteBufAllocator getAllocator() {
            return callback.getAllocator();
          }
        };

    backwardSyncLogResponseReduceCollector
        .register(new InstanceId(request.getUnits(0).getMyself()), request, methodCallback);
    for (PbBackwardSyncLogRequestUnit syncLogRequestUnit : request.getUnitsList()) {
      SegId segId = new SegId(syncLogRequestUnit.getVolumeId(), syncLogRequestUnit.getSegIndex());
      SegmentUnit segmentUnit = segmentUnitManager.get(segId);
      if (segmentUnit != null) {
        SyncLogReceiver syncLogTask = new SyncLogReceiver(request.getRequestId(), context,
            instanceStore, plalEngine, cfg, archiveManager,
            this.service.getDataNodeSyncClientFactory(), segmentUnitManager,
            syncLogRequestUnit, backwardSyncLogResponseReduceCollector);
        if (!syncLogTaskExecutor.submit(syncLogTask)) {
          syncLogTask.fail(PbErrorCode.SERVICE_IS_BUSY);
        }
      } else {
        SyncLogReceiver syncLogTask = new SyncLogReceiver(request.getRequestId(), context,
            instanceStore, plalEngine, cfg, archiveManager,
            this.service.getDataNodeSyncClientFactory(), segmentUnitManager,
            syncLogRequestUnit, backwardSyncLogResponseReduceCollector);
        syncLogTask.fail(PbErrorCode.SEGMENT_NOT_FOUND);
      }
    }
  }

  @Override
  public void syncLog(PbAsyncSyncLogsBatchRequest request,
      MethodCallback<PbAsyncSyncLogsBatchResponse> callback) {
    logger.debug("got a sync log batch request {}", request);
    if (shutdown) {
      callback.fail(new ServerShutdownException(
          "myself : " + context.getInstanceId() + " request option" + request));
      return;
    }
    if (pauseRequestProcessing) {
      callback.fail(new NetworkUnhealthyException());
      return;
    }

    waitingSyncLogRequestMeter.mark();
    waitingSyncLogRequestCount.incCounter();
    MethodCallback<PbAsyncSyncLogsBatchResponse> methodCallback =
        new MethodCallback<PbAsyncSyncLogsBatchResponse>() {
          @Override
          public void complete(PbAsyncSyncLogsBatchResponse object) {
            waitingSyncLogRequestCount.decCounter();
            logger.info("return a sync log batch response {}", request.getRequestId());
            callback.complete(object);
          }

          @Override
          public void fail(Exception e) {
            waitingSyncLogRequestCount.decCounter();
            logger.warn("return failed a sync log batch response {}", request.getRequestId());
            callback.fail(e);
          }

          @Override
          public ByteBufAllocator getAllocator() {
            return callback.getAllocator();
          }
        };

    for (PbAsyncSyncLogBatchUnit batchUnit : request.getSegmentUnitsList()) {
      SegId segId = new SegId(batchUnit.getVolumeId(), batchUnit.getSegIndex());
      SegmentUnit segmentUnit = segmentUnitManager.get(segId);

      if (segmentUnit != null) {
        SyncLogTask syncLogTask = new SyncLogPusher(segmentUnitManager, batchUnit, context,
            service, backwardSyncLogRequestReduceCollector,
            this.service.getLogStorageReader(),
            this.cfg);
        logger.debug("new a sync log pusher {} and submit to task executor", syncLogTask);
        syncLogTaskExecutor.submit(syncLogTask);
      }
    }

    PbAsyncSyncLogsBatchResponse.Builder builder = PbAsyncSyncLogsBatchResponse.newBuilder();
    builder.setRequestId(request.getRequestId());
    methodCallback.complete(builder.build());
  }

  @Override
  public void check(PbCheckRequest request, MethodCallback<PbCheckResponse> callback) {
    if (shutdown) {
      callback.fail(new ServerShutdownException(
          "myself : " + context.getInstanceId() + " request option" + request.getRequestOption()));
      return;
    }
    if (pauseRequestProcessing) {
      callback.fail(new NetworkUnhealthyException());
      return;
    }

    SegId segId = new SegId(request.getVolumeId(), request.getSegIndex());
    logger.warn("check request got {}, i am {}, seg {}", request, context.getInstanceId(), segId);
    SegmentUnit segUnit = segmentUnitManager.get(segId);
    if (segUnit == null) {
      logger.error("segment not found for segId={} request option={}", segId,
          request.getRequestOption());
      callback.fail(NettyExceptionHelper.buildSegmentNotFoundException(segId, context));
      return;
    }
    SegmentUnitStatus status = segUnit.getSegmentUnitMetadata().getStatus();
    if (status.hasGone()) {
      logger.error("segment status {} for segId={} request option={}", status, segId,
          request.getRequestOption());
      callback.fail(NettyExceptionHelper.buildSegmentDeleteException(segId, context));
      return;
    }

    checkEngine.submit(new IoTask(segUnit) {
      @Override
      public void run() {
        try {
          switch (request.getRequestOption()) {
            case CHECK_PRIMARY:
              checkPrimaryOrTempPrimaryReachable(request, callback, segUnit, true);
              break;
            case CHECK_SECONDARY:
              checkSecondaryReachable(request, callback, segUnit, true);
              break;
            case CONFIRM_TP_UNREACHABLE:
              confirmPrimaryOrTempPrimaryUnreachable(request, callback, segUnit, false);
              break;
            case CONFIRM_UNREACHABLE:
              confirmPrimaryOrTempPrimaryUnreachable(request, callback, segUnit, true);
              break;
            case CHECK_TEMP_PRIMARY:
              checkPrimaryOrTempPrimaryReachable(request, callback, segUnit, false);
              break;
            case TP_CHECK_SECONDARY:
              checkSecondaryReachable(request, callback, segUnit, true);
              break;
            case SECONDARY_CHECK_SECONDARY:
              checkSecondaryReachable(request, callback, segUnit, false);
              break;
            default:
          }
        } catch (Throwable t) {
          logger.error("caught a throwable when check {}", request.getRequestOption(), t);
          callback.fail(NettyExceptionHelper.buildServerProcessException(t.toString()));
        }
      }
    });

  }

  protected void confirmPrimaryOrTempPrimaryUnreachable(PbCheckRequest request,
      MethodCallback<PbCheckResponse> callback, SegmentUnit segUnit, boolean primaryOrTempPrimary) {
    SegId segId = segUnit.getSegId();

    final SegmentMembership myMembership = segUnit.getSegmentUnitMetadata().getMembership();
    final SegmentMembership requestMembership = PbRequestResponseHelper
        .buildMembershipFrom(request.getRequestPbMembership());

    SegmentUnitStatus status = segUnit.getSegmentUnitMetadata().getStatus();
    if (primaryOrTempPrimary && !segUnit.isSecondaryZombie()) {
      logger.error("when confirm p, i am not in secondary zombie status {} {}!! ",
          segUnit.isSecondaryZombie(), segUnit.isSecondaryZombieAgain());
      callback
          .fail(NettyExceptionHelper.buildNotSecondaryZombieException(segId, status, myMembership));
      return;
    }

    if (!primaryOrTempPrimary && !segUnit.isSecondaryZombieAgain()) {
      logger.error("when confirm tp, i am not in secondary zombie status again {} {}!! ",
          segUnit.isSecondaryZombie(), segUnit.isSecondaryZombieAgain());
      callback
          .fail(NettyExceptionHelper.buildNotSecondaryZombieException(segId, status, myMembership));
      return;
    }
   
    PbCheckResponse.Builder builder = PbCheckResponse.newBuilder();
    builder.setRequestId(request.getRequestId());

    if (!request.hasTempPrimary() || request.getTempPrimary() == 0) {
      logger.error("no temp primary is set !! {}", request);
      callback.fail(NettyExceptionHelper.buildServerProcessException("set temp primary !!"));
    }

    InstanceId tempPrimary = new InstanceId(request.getTempPrimary());

    if (!requestMembership.isSecondary(tempPrimary)) {
      callback.fail(NettyExceptionHelper
          .buildServerProcessException("the given temp primary is not secondary in membership"));
      return;
    }

    boolean membershipUpdated = false;
    if (myMembership.compareTo(requestMembership) > 0) {
      logger.warn("the requester has a lower membership at {}. Ours: {}, Theirs: {}", segId,
          myMembership,
          requestMembership);
      if (myMembership.compareTo(requestMembership.secondaryBecomeTempPrimary(tempPrimary)) == 0) {
        membershipUpdated = true;
        logger.warn("it is ok that the requset membership lower than mine");
      } else {
        callback
            .fail(NettyExceptionHelper.buildMembershipVersionLowerException(segId, myMembership));
        return;
      }
    }
   
    boolean isTempPrimary = context.getInstanceId().getId() == request.getTempPrimary();

    int incGeneration = segUnit.confirmSecondaryZombie(!primaryOrTempPrimary, tempPrimary);
    boolean successConfirm = incGeneration > 0;
    if (!successConfirm) {
      logger.warn("secondary has already confirmed primary unreachable {}", segId);
      if (membershipUpdated) {
        builder.setPbMembership(PbRequestResponseHelper.buildPbMembershipFrom(myMembership));
      }
      callback.complete(builder.build());
      return;
    }

    SegmentMembership newMembership;

    if (myMembership.isTempPrimary(tempPrimary)) {
      newMembership = myMembership;
    } else {
      Validate.isTrue(!membershipUpdated);
      newMembership = myMembership.secondaryBecomeTempPrimary(incGeneration, tempPrimary);
      if (!primaryOrTempPrimary && myMembership.getTempPrimary() != null) {
       
        newMembership = newMembership
            .aliveSecondaryBecomeInactive(myMembership.getTempPrimary());
      }
    }

    if (newMembership == null || !tempPrimary.equals(newMembership.getTempPrimary())) {
      logger.error("wrong temp primary {} for {} membership {}", tempPrimary, segId, myMembership);
      callback.fail(NettyExceptionHelper.buildServerProcessException("wrong temp primary"));
      return;
    }

    if (isTempPrimary) {
      if (segUnit.getSegmentUnitMetadata().getMigrationStatus() != MigrationStatus.NONE) {
        logger.error("this could not happen, migrating segment unit can't be temp primary {}",
            segUnit);
        callback.fail(NettyExceptionHelper
            .buildServerProcessException("migrating segment unit can't be temp primary"));
        return;
      }
      try {
        LogIdGenerator logIdGenerator = new LogIdGenerator(
            segUnit.getSegmentUnitMetadata().getMembership().getSegmentVersion().getEpoch(), true,
            !primaryOrTempPrimary);
        logger.warn("{} temp primary set a jumped logIdGenerator", segId);
        service.getMutationLogManager().setLogIdGenerator(segId, logIdGenerator);
      } catch (Exception e) {
        logger.error("exception caught", e);
        callback.fail(NettyExceptionHelper.buildServerProcessException(e.toString()));
        return;
      }
    } else {
     
     
      newMembership = null;
    }

    SegmentUnitStatus newStatus;
    boolean lockSuccess = false;
    try {
      lockSuccess = segUnit.tryLockStatus(500, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      logger.error("", e);
    }
    if (!lockSuccess) {
      logger.error("update metadata lock fail");
      callback
          .fail(NettyExceptionHelper.buildMembershipVersionHigerException(segId, newMembership));
      return;
    }
   
    if (status == SegmentUnitStatus.PreSecondary || status == SegmentUnitStatus.ModeratorSelected 
        || status == SegmentUnitStatus.SecondaryEnrolled
        || status == SegmentUnitStatus.SecondaryApplicant) {
      long oldPrimary = request.getCheckInstanceId();
      if (oldPrimary == segUnit.getPotentialPrimaryId()) {
        newStatus = SegmentUnitStatus.Start;
      } else {
        logger.warn(" {} I am already voting with {} , no need to change to Start", segId,
            segUnit.getPotentialPrimaryId());
        newStatus = null;
      }
    } else if (status != SegmentUnitStatus.Start && status != SegmentUnitStatus.PrePrimary) {
      newStatus = SegmentUnitStatus.Start;
    } else {
      logger.warn(" {} I am already in {} status, no need to change to Start", segId, status);
      newStatus = null;
    }

    boolean persistForce = false;
    try {
      logger.warn("{} confirm primary unreachable, the latest membership {}", segId, newMembership);
      if (cfg.isArbiterSyncPersistMembershipOnIoPath() && segUnit.isArbiter()) {
        persistForce = archiveManager
            .asyncUpdateSegmentUnitMetadata(segId, newMembership, newStatus);
      } else {
       
       
       
        if (segUnit.isArbiter()) {
          logger.warn("{} update membership but will be persisted asyncly", segId);
        }
        archiveManager.asyncUpdateSegmentUnitMetadata(segId, newMembership, newStatus);
      }

      logger.warn("{} done updating segment unit metadata.", segId);
      if (isTempPrimary) {
        MembershipPusher.getInstance(context.getInstanceId()).submit(segId);
        builder.setPbMembership(PbRequestResponseHelper.buildPbMembershipFrom(newMembership));
      }
    } catch (Exception e) {
      logger.error("cannot update secondary zombie {} to Start", segId, e);
      callback.fail(NettyExceptionHelper.buildServerProcessException(e.toString()));
      return;
    } finally {
      segUnit.unlockStatus();
    }

    if (persistForce) {
      segUnit.getArchive().persistSegmentUnitMeta(segUnit.getSegmentUnitMetadata(), true);
    }
    callback.complete(builder.build());
  }

  protected void checkPrimaryOrTempPrimaryReachable(PbCheckRequest request,
      MethodCallback<PbCheckResponse> callback,
      SegmentUnit segUnit, boolean primaryOrTempPrimary) {
   
    SegId segId = segUnit.getSegId();
    SegmentUnitStatus status = segUnit.getSegmentUnitMetadata().getStatus();
    SegmentMembership membership = segUnit.getSegmentUnitMetadata().getMembership();
    SegmentMembership requestMembership = PbRequestResponseHelper
        .buildMembershipFrom(request.getRequestPbMembership());
    int compareResult = membership.compareTo(requestMembership);
    if (compareResult > 0) {
      callback.fail(NettyExceptionHelper.buildMembershipVersionLowerException(segId, membership));
      return;
    } else if (compareResult < 0) {
      logger.warn(
          " {} my membership is even lower, mine : {}, the requested {}, zombie {}, update it",
          segId, membership,
          requestMembership, segUnit.isSecondaryZombie());
     
     
     
     
     
      boolean lockSuccess = false;
      try {
        lockSuccess = segUnit.tryLockStatus(500, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        logger.error("", e);
      }
      if (!lockSuccess) {
        logger.error("update metadata lock fail");
        callback.fail(NettyExceptionHelper.buildMembershipVersionHigerException(segId, membership));
        return;
      }
      boolean persistForce = false;
      try {
        if (cfg.isArbiterSyncPersistMembershipOnIoPath() && segUnit.isArbiter()) {
          persistForce = archiveManager
              .asyncUpdateSegmentUnitMetadata(segId, requestMembership, SegmentUnitStatus.Start);
        } else {
          if (segUnit.getSegmentUnitMetadata().getVolumeType() != VolumeType.REGULAR) {
            logger.warn("{} update membership but will be persisted asyncly", segId);
          }
          archiveManager
              .asyncUpdateSegmentUnitMetadata(segId, requestMembership, SegmentUnitStatus.Start);
        }
        membership = requestMembership;
      } catch (Exception e) {
        logger.error("", e);
        callback.fail(NettyExceptionHelper.buildServerProcessException(e.getClass().getName()));
        return;
      } finally {
        segUnit.unlockStatus();
      }

      if (persistForce) {
        segUnit.getArchive().persistSegmentUnitMeta(segUnit.getSegmentUnitMetadata(), true);
      }
    }

    InstanceId instanceId = new InstanceId(request.getCheckInstanceId());
    if (primaryOrTempPrimary && !membership.isPrimary(instanceId)) {
      logger
          .error("why is the given ID {} not primary in membership {}", instanceId, membership);
      callback.fail(NettyExceptionHelper.buildNotPrimaryException(segId, status, membership));
      return;
    }
    if (!primaryOrTempPrimary && !membership.isTempPrimary(instanceId)) {
      logger.error("why is the given ID {} not tempPrimaryID in membership {}", instanceId,
          membership);
      callback.fail(NettyExceptionHelper.buildNotTempPrimaryException(segId, status, membership));
      return;
    }

    Instance instance = instanceStore.get(instanceId);
    if (instance == null) {
      callback.fail(NettyExceptionHelper
          .buildServerProcessException("given pOrTp in instance store is null"));
      return;
    }

    PbCheckResponse.Builder builder = PbCheckResponse.newBuilder();
    builder.setRequestId(request.getRequestId());
    builder.setMyInstanceId(context.getInstanceId().getId());

    boolean alreadyZombie = false;
    if (primaryOrTempPrimary && segUnit.isSecondaryZombie()) {
      logger.warn(
          "I am already in secondary zombie status, don't need "
              + "to check primary again, just return unreachable");
      alreadyZombie = true;
    }

    if (!primaryOrTempPrimary) {
      if (!segUnit.isSecondaryZombie()) {
        logger
            .error("i am not in secondary zombie status,"
                    + " can't do check tp seg {} status {}", segId,
                status);
        callback
            .fail(NettyExceptionHelper.buildNotSecondaryZombieException(segId, status, membership));
        return;
      }

      if (segUnit.isSecondaryZombieAgain()) {
        logger.warn(
            "I am already in secondary zombie status again, "
                + "don't need to check temp primary again, just return unreachable");
        alreadyZombie = true;
      }
    }

    boolean isMigrating =
        membership.isJoiningSecondary(context.getInstanceId()) || segUnit.isMigrating();
    if (alreadyZombie) {
      builder.setReachable(false);
      builder.setMigrating(isMigrating);
      long pcl = status == SegmentUnitStatus.PrePrimary ? Long.MAX_VALUE
          : segUnit.getPclWhenBecomeSecondaryZombie();
      logger.warn("status {} pcl {} i am {}", status, pcl, context.getInstanceId());
      builder.setPcl(pcl);
      callback.complete(builder.build());
      return;
    }

    boolean reachable;
    if (request.hasMemberHasGone() && request.getMemberHasGone()) {
      reachable = false;
    } else {
      reachable = isMemberReachable(instance);
      logger.warn("check primary or temp p result is {}", reachable);
    }

    if (!reachable) {
      if (primaryOrTempPrimary) {
        segUnit.markSecondaryZombie();
        long pcl = segUnit.isArbiter()
            ? (LogImage.IMPOSSIBLE_LOG_ID) :
            (segUnit.getSegmentLogMetadata().getClId());

        if (status == SegmentUnitStatus.PrePrimary) {
          Validate.isTrue(!isMigrating, "my status is %s", status);
         
          logger.warn("As a prePrimary, set pcl to max value");
          pcl = Long.MAX_VALUE;
        }

        logger.warn("primary unreachable, mark myself as a secondary zombie pcl {} segId={}", pcl,
            segUnit.getSegId());
        segUnit.setPclWhenBecomeSecondaryZombie(pcl);
      } else {
        logger.warn("temp primary unreachable, mark myself as a secondary zombie again segId={}",
            segUnit.getSegId());
        segUnit.markSecondaryZombieAgain();
      }

      builder.setPcl(segUnit.getPclWhenBecomeSecondaryZombie());
      logger.warn("give coorinator my pcl {}, and migrating {}", builder.getPcl(), isMigrating);
    }

    builder.setReachable(reachable);
    builder.setMigrating(isMigrating);
    callback.complete(builder.build());
  }

  /**
   * checkSecondaryReachable.
   *
   * @param becomeInactive if the secondary to check is unreachable, need to call become inactive in
   *                       membership
   */
  protected void checkSecondaryReachable(PbCheckRequest request,
      MethodCallback<PbCheckResponse> callback,
      SegmentUnit segUnit, boolean becomeInactive) {
    SegId segId = segUnit.getSegId();
    SegmentUnitMetadata segmentUnitMetadata = segUnit.getSegmentUnitMetadata();
    SegmentUnitStatus status = segmentUnitMetadata.getStatus();
    SegmentMembership membership = segmentUnitMetadata.getMembership();
    SegmentMembership requestMembership = PbRequestResponseHelper
        .buildMembershipFrom(request.getRequestPbMembership());
    if (membership.compareTo(requestMembership) > 0) {
      callback.fail(NettyExceptionHelper.buildMembershipVersionLowerException(segId, membership));
      return;
    }

    InstanceId instanceIdOfmemberToCheck = new InstanceId(request.getCheckInstanceId());
    if (membership.getAllSecondaries().stream()
        .noneMatch(instanceId -> instanceId.equals(instanceIdOfmemberToCheck))) {
      callback.fail(NettyExceptionHelper.buildMembershipVersionLowerException(segId, membership));
      return;
    }

    switch (request.getRequestOption()) {
      case CHECK_SECONDARY:
       
        if (!membership.isPrimary(context.getInstanceId()) || !status.isPrimary()) {
          logger.error("I am not primary ! {} {}", status, segId);
          callback.fail(NettyExceptionHelper
              .buildNotPrimaryException(segId, segmentUnitMetadata.getStatus(), membership));
          return;
        }
        break;
      case TP_CHECK_SECONDARY:
       
        if (!membership.isSecondary(context.getInstanceId()) || !membership
            .isTempPrimary(context.getInstanceId())) {
          logger
              .error("I {} am not tp! {} {} membership {}", context.getInstanceId(), status, segId,
                  membership);
          callback.fail(NettyExceptionHelper
              .buildNotTempPrimaryException(segId, segmentUnitMetadata.getStatus(), membership));
          return;
        }
        break;
      case SECONDARY_CHECK_SECONDARY:
       
       
        break;
      default:
    }
   
    PbCheckResponse.Builder builder = PbCheckResponse.newBuilder();
    builder.setRequestId(request.getRequestId());
    if (membership.getInactiveSecondaries().contains(instanceIdOfmemberToCheck)) {
      logger.error(
          "the required secondary is already inactive, "
              + "maybe the requester doesn't have the latest membership ? {}",
          segId);
      builder.setReachable(false);
      builder.setPbMembership(PbRequestResponseHelper.buildPbMembershipFrom(membership));
      callback.complete(builder.build());
      return;
    }

    Instance secondaryInstance = instanceStore.get(instanceIdOfmemberToCheck);
    if (secondaryInstance == null) {
      callback.fail(
          NettyExceptionHelper
              .buildServerProcessException("given secondary in instance store is null"));
      return;
    }

    boolean reachable = true;
    boolean secondaryHasGone = request.hasMemberHasGone() && request.getMemberHasGone();
    if (secondaryHasGone) {
      reachable = false;
      logger.warn("secondaryHasGone!");
    } else {
      reachable = isMemberReachable(secondaryInstance);
      logger.warn("check secondary result is {}", reachable);
    }

    if (reachable) {
      builder.setReachable(true);
      callback.complete(builder.build());
      return;
    } else {
      if (becomeInactive) {
        trySecondaryBecomeInactive(segUnit, instanceIdOfmemberToCheck, callback, builder);
      } else {
        builder.setReachable(false);
        callback.complete(builder.build());
      }
    }
  }

  private void trySecondaryBecomeInactive(SegmentUnit segUnit, InstanceId unreachableSecondary,
      MethodCallback<PbCheckResponse> callback, PbCheckResponse.Builder builder) {
   
   
   
   
   
    SegId segId = segUnit.getSegId();
    SegmentMembership membership = segUnit.getSegmentUnitMetadata().getMembership();

    boolean stillHaveEnoughSupport = false;
    try {
      stillHaveEnoughSupport = porTpStillHaveEnoughSupport(segId, membership, unreachableSecondary);
      logger.warn("primary still have enough supports ? {} {}", segId, stillHaveEnoughSupport);
    } catch (Exception e) {
      callback.fail(NettyExceptionHelper.buildServerProcessException(e.getMessage()));
      return;
    }

    if (!stillHaveEnoughSupport) {
      logger.warn("It is dangerous to accept more write requests for segment unit {}", segUnit);
      callback.fail(new IncompleteGroupException());
      return;
    } else {
      logger.warn("canRemoveMember {}", unreachableSecondary);
      SegmentMembership newMembership;
      InstanceId instanceIdToRemove = unreachableSecondary;
      if (membership.isArbiter(instanceIdToRemove)) {
        newMembership = membership.arbiterBecomeInactive(instanceIdToRemove);
      } else {
        newMembership = membership.aliveSecondaryBecomeInactive(instanceIdToRemove);
        SegmentLogMetadata segmentLogMetadata = segUnit.getSegmentLogMetadata();
        segmentLogMetadata.removePeerClId(instanceIdToRemove);
        segmentLogMetadata.removePeerPlId(instanceIdToRemove);
        if (segUnit.getSegmentUnitMetadata().getVolumeType() == VolumeType.REGULAR) {
          segmentLogMetadata.setPrimaryMaxLogIdWhenPsi(segmentLogMetadata.getMaxLogId());
          logger.warn("segment {} update PrimaryMaxLogIDWhenPSI {} pcl {}", segUnit,
              segmentLogMetadata.getPrimaryMaxLogIdWhenPsi(),
              segmentLogMetadata.getClId());
        }
      }

      if (newMembership == null) {
        logger.error(
            "why is the new membership null ? new membership,"
                + " current membership and instance to be removed : {}, {}, {} ",
            newMembership, membership, instanceIdToRemove);
        callback.fail(NettyExceptionHelper.buildServerProcessException("memberhsip not updated"));
        return;
      }
      boolean lockSuccess = false;
      try {
        lockSuccess = segUnit.tryLockStatus(500, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        logger.error("", e);
      }
      if (!lockSuccess) {
        logger.error("update metadata lock fail");
        callback.fail(NettyExceptionHelper.buildMembershipVersionHigerException(segId, membership));
        return;
      }
      boolean persistForce = false;
      try {
        if (cfg.isArbiterSyncPersistMembershipOnIoPath() && segUnit.isArbiter()) {
          persistForce = archiveManager.asyncUpdateSegmentUnitMetadata(segId, newMembership, null);
        } else {
          if (segUnit.getSegmentUnitMetadata().getVolumeType() != VolumeType.REGULAR) {
            logger.warn("{} update membership but will be persisted asyncly", segId);
          }
          archiveManager.asyncUpdateSegmentUnitMetadata(segId, newMembership, null);
        }
        MembershipPusher.getInstance(context.getInstanceId()).submit(segId);
      } catch (Exception e) {
        logger.error("cannot update membership from {} to {}", membership, newMembership, e);
        callback.fail(NettyExceptionHelper
            .buildServerProcessException("internal error in data node : cannot update membership"));
        return;
      } finally {
        segUnit.unlockStatus();
      }
      if (persistForce) {
        segUnit.getArchive().persistSegmentUnitMeta(segUnit.getSegmentUnitMetadata(), true);
      }
      builder.setPbMembership(PbRequestResponseHelper.buildPbMembershipFrom(newMembership));
    }

    builder.setReachable(false);
    callback.complete(builder.build());
  }

  private boolean porTpStillHaveEnoughSupport(SegId segId, SegmentMembership membership,
      InstanceId toRemove) {
    Set<InstanceId> aliveMembers = membership.getAliveSecondaries();
    Set<InstanceId> membersToCheck = Sets.newHashSet();
    for (InstanceId member : aliveMembers) {
      if (!member.equals(toRemove) && !member.equals(membership.getTempPrimary())) {
        membersToCheck.add(member);
      }
    }

    VolumeType volumeType = segmentUnitManager.get(segId).getSegmentUnitMetadata().getVolumeType();
    int quorumSizeExceptMe = volumeType.getVotingQuorumSize() - 1;
    if (membersToCheck.size() < quorumSizeExceptMe) {
      logger.warn(
          "there is not enough MEMBER {} in membership except"
              + " for the given secondary (and me if tp),"
              + " though the secondary is unreachable, "
              + "I need {} so can't give a replay, segId {}",
          membersToCheck.size(), quorumSizeExceptMe, segId);
      return false;
    } else {
      int reachableCount = 0;
      for (InstanceId instanceIdToCheck : membersToCheck) {
        Instance instanceToCheck = instanceStore.get(instanceIdToCheck);
        if (instanceToCheck == null) {
          logger.warn("a secondary {} in instance store is null", instanceIdToCheck);
          continue;
        } else {
          logger.warn("is tp = {} or p going to check another secondary {}, segid = {}",
              membership.getTempPrimary(), instanceToCheck, segId);
          boolean reachable = isMemberReachable(instanceToCheck);
          reachableCount += reachable ? 1 : 0;
          logger.warn("result is {}, support count {}", reachable, reachableCount);
        }
      }
      return reachableCount >= quorumSizeExceptMe;
    }
  }

  private boolean isMemberReachable(Instance memberToCheck) {
    EndPoint pointByServiceName = memberToCheck.getEndPointByServiceName(PortType.IO);
    boolean reachable = NetworkIoHealthChecker.INSTANCE
        .blockingCheck(pointByServiceName.getHostName());
    logger.warn("check if {} io {} is reachable {}", memberToCheck, pointByServiceName, reachable);
    return reachable;
  }

  private PyTimerContext getReadDelayTimer() {
    if (PyMetricRegistry.getMetricRegistry().isEnableMetric()) {
      return timerReadData.time();
    } else {
      return new PyTimerContextImpl(readDelayTimer.time());
    }
  }

  private PyTimerContext getWriteDelayTimer() {
    if (PyMetricRegistry.getMetricRegistry().isEnableMetric()) {
      return timerWriteData.time();
    } else {
      return new PyTimerContextImpl(writeDelayTimer.time());
    }
  }

  private void reportReadTimeDelay(long nanoTimeDelay, long requestId, SegId segId) {
    costWarning(nanoTimeDelay, requestId, segId, true);
    averageIoDelayCollector.submit(nanoTimeDelay);
  }

  private void reportWriteTimeDelay(long nanoTimeDelay, long requestId, SegId segId) {
    costWarning(nanoTimeDelay, requestId, segId, false);
    averageIoDelayCollector.submit(nanoTimeDelay);
  }

  private void costWarning(long nanoTimeDelay, long requestId, SegId segId, boolean readOrWrite) {
    if (cfg.isEnableFrontIoCostWarning()) {
      long millisTimeDelay = TimeUnit.NANOSECONDS.toMillis(nanoTimeDelay);
      if (millisTimeDelay > cfg.getFrontIoTaskThresholdMs()) {
        logger.warn(
            "[IO Cost Warning] {} request cost too long: [{}] MS, request id [{}], seg id [{}]",
            readOrWrite ? "read" : "write",
            millisTimeDelay, requestId, segId);
      }
    }
  }

  public ConsumerService<DelayedIoTask> getCopyEngine() {
    return copyEngine;
  }

  public ByteBufAllocator getByteBufAllocator() {
    return byteBufAllocator;
  }

  public void setByteBufAllocator(ByteBufAllocator byteBufAllocator) {
    this.byteBufAllocator = byteBufAllocator;
  }

  public GenericAsyncClientFactory<AsyncDataNode.AsyncIface> getAsyncClientFactory() {
    return asyncClientFactory;
  }

  public void setAsyncClientFactory(
      GenericAsyncClientFactory<AsyncDataNode.AsyncIface> asyncClientFactory) {
    this.asyncClientFactory = asyncClientFactory;
  }

  public DataNodeConfiguration getDataNodeConfiguration() {
    return cfg;
  }

  public void pauseRequestProcessing() {
    pauseRequestProcessing = true;
  }

  public void reviveRequestProcessing() {
    pauseRequestProcessing = false;
  }

  public void setIoThrottleManager(IoThrottleManager ioThrottleManager) {
    this.ioThrottleManager = ioThrottleManager;
  }

  public AlarmReporter getAlarmReporter() {
    return alarmReporter;
  }

  public void setAlarmReporter(AlarmReporter alarmReporter) {
    this.alarmReporter = alarmReporter;

    if (alarmReporter == null) {
      return;
    }

    final long rateMs = cfg.getAlarmReportRateMs();

    alarmReporter.register(() -> {
      TimeUnit ioDelayTimeUnit = cfg.getResolvedIoDelayTimeunit();
      long delay = ioDelayTimeUnit.convert(averageIoDelayCollector.average(), TimeUnit.NANOSECONDS);

      return AlarmReportData.generateIoDelayAlarm(delay, ioDelayTimeUnit);
    }, rateMs);

    alarmReporter.register(() -> AlarmReportData
            .generateQueueSizeAlarm(maxWriteQueueSizeCollector.getAndResetMaxValue(),
                "write_queue"),
        rateMs);

    alarmReporter.register(() -> AlarmReportData
            .generateQueueSizeAlarm(maxGiveYouLogIdQueueSizeCollector.getAndResetMaxValue(),
                "give_you_log_id_queue"),
        rateMs);

    alarmReporter.register(() -> AlarmReportData
            .generateQueueSizeAlarm(maxReadQueueSizeCollector.getAndResetMaxValue(), "read_queue"),
        rateMs);

    alarmReporter.register(() -> AlarmReportData
            .generateQueueSizeAlarm(maxPendingRequestQueueSizeCollector.getAndResetMaxValue(),
                "pending_request_queue"),
        rateMs);
  }

  public void setBackwardSyncLogRequestReduceCollector(
      SyncLogReduceCollector<PbBackwardSyncLogRequestUnit,
          PbBackwardSyncLogsRequest, PbBackwardSyncLogsRequest>
          backwardSyncLogRequestReduceCollector) {
    this.backwardSyncLogRequestReduceCollector = backwardSyncLogRequestReduceCollector;
  }

  public void setBackwardSyncLogResponseReduceCollector(
      SyncLogReduceCollector<PbBackwardSyncLogResponseUnit,
          PbBackwardSyncLogsResponse, PbBackwardSyncLogsRequest>
          backwardSyncLogResponseReduceCollector) {
    this.backwardSyncLogResponseReduceCollector = backwardSyncLogResponseReduceCollector;
  }

  public void setSyncLogBatchRequestReduceCollector(
      SyncLogReduceCollector<PbAsyncSyncLogBatchUnit,
          PbAsyncSyncLogsBatchRequest, PbAsyncSyncLogsBatchRequest>
          syncLogBatchRequestReduceCollector) {
    this.syncLogBatchRequestReduceCollector = syncLogBatchRequestReduceCollector;
  }

  public void setSyncLogTaskExecutor(
      SyncLogTaskExecutor syncLogTaskExecutor) {
    this.syncLogTaskExecutor = syncLogTaskExecutor;
  }

  public class GiveYouLogIdTask extends IoTask {
    final GiveYouLogIdRequest request;
    final MethodCallback<GiveYouLogIdResponse> callback;
    final PyTimerContext waitTime;
    final PyTimerContext overallTime;
    final Timeout timeoutTask;

    GiveYouLogIdTask(SegmentUnit segmentUnit, GiveYouLogIdRequest request,
        MethodCallback<GiveYouLogIdResponse> callback) {
      super(segmentUnit);
      this.request = request;
      this.waitTime = timerGiveYouLogIdWait.time();
      this.overallTime = timerGiveYouLogId.time();
      waitingGiveLogIdRequestCount.incCounter();
      this.timeoutTask = giveLogIdHash.newTimeout(new io.netty.util.TimerTask() {
        @Override
        public void run(Timeout timeout) throws Exception {
          try {
            SegmentLogMetadata segmentLogMetadata = segmentUnit.getSegmentLogMetadata();
            for (Broadcastlog.LogUuidAndLogId giveIdTask : request.getLogUuidAndLogIdsList()) {
              logger.warn("fq the task is segId {} {} {}", segmentUnit.getSegId(), giveIdTask,
                  segmentLogMetadata.getCompleteLog(giveIdTask.getLogUuid()));
            }
          } catch (Throwable e) {
            logger.error("", e);
          }
        }
      }, 20, TimeUnit.SECONDS);
      this.callback = new AbstractMethodCallback<GiveYouLogIdResponse>() {
        @Override
        public void complete(GiveYouLogIdResponse object) {
          waitingGiveLogIdRequestCount.decCounter();
          if (cfg.isEnableFrontIoCostWarning()) {
            long time = TimeUnit.NANOSECONDS.toMillis(overallTime.stop());
            if (time > cfg.getFrontIoTaskThresholdMs()) {
              logger.warn(
                  "[IO Cost Warning] give you log id cost too long: [{}] MS, seg id [{}]",
                  time, segmentUnit.getSegId());
            }
          }
          callback.complete(object);
          timeoutTask.cancel();
        }

        @Override
        public void fail(Exception e) {
          waitingGiveLogIdRequestCount.decCounter();
          overallTime.stop();
          callback.fail(e);
          timeoutTask.cancel();
        }
      };
    }

    @Override
    public void run() {
      waitTime.stop();

      PyTimerContext taskTime = timerGiveYouLogIdProcess.time();

      long primaryPclId = LogImage.INVALID_LOG_ID;
      if (request.hasPcl()) {
        primaryPclId = request.getPcl();
      }
     
      GiveYouLogIdEvent giveYouLogIdEvent = new GiveYouLogIdEvent(
          context.getInstanceId().getId(), callback, request.getLogUuidAndLogIdsList(),
          segmentUnit, request.getMyInstanceId());
      try {
        SegmentLogMetadata segmentLogMetadata = segmentUnit.getSegmentLogMetadata();
        segmentLogMetadata.giveYouLogId(giveYouLogIdEvent, plalEngine);
        segmentUnit.setPrimaryClIdAndCheckNeedPclDriverOrNot(primaryPclId);
      } catch (Exception e) {
        logger.error("give you log id error", e);
        callback.fail(e);
      }
      taskTime.stop();
    }
  }
}
