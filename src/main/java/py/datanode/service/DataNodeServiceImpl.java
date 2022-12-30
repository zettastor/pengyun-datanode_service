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

import static py.archive.segment.SegmentUnitBitmap.SegmentUnitBitMapType.Data;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.SettableFuture;
import io.netty.buffer.ByteBufAllocator;
import io.netty.util.HashedWheelTimer;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.nio.channels.CompletionHandler;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang3.Validate;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.PbRequestResponseHelper;
import py.RequestResponseHelper;
import py.app.context.AppContext;
import py.app.thrift.ThriftAppEngine;
import py.app.thrift.ThriftProcessorFactory;
import py.archive.AbstractArchiveBuilder;
import py.archive.Archive;
import py.archive.ArchiveOptions;
import py.archive.ArchiveStatus;
import py.archive.ArchiveType;
import py.archive.PluginPlugoutManager;
import py.archive.page.PageAddress;
import py.archive.segment.MigrationStatus;
import py.archive.segment.SegId;
import py.archive.segment.SegmentUnitMetadata;
import py.archive.segment.SegmentUnitStatus;
import py.archive.segment.SegmentUnitType;
import py.archive.segment.SegmentVersion;
import py.archive.segment.recurring.SegmentUnitTaskExecutor;
import py.archive.segment.recurring.SegmentUnitTaskExecutorImpl;
import py.client.thrift.GenericThriftClientFactory;
import py.common.LoggerTracer;
import py.common.ReflectionUtils;
import py.common.RequestIdBuilder;
import py.common.struct.EndPoint;
import py.common.struct.Pair;
import py.consumer.ConsumerService;
import py.consumer.ConsumerServiceDispatcher;
import py.consumer.SingleThreadConsumerService;
import py.datanode.archive.RawArchive;
import py.datanode.archive.RawArchiveBuilder;
import py.datanode.archive.RawArchiveManager;
import py.datanode.archive.UnsettledArchive;
import py.datanode.archive.UnsettledArchiveManager;
import py.datanode.client.DataNodeServiceAsyncClientWrapper;
import py.datanode.client.DataNodeServiceAsyncClientWrapper.BroadcastResult;
import py.datanode.configuration.DataNodeConfiguration;
import py.datanode.exception.AcceptedProposalTooOldException;
import py.datanode.exception.AcceptorFrozenException;
import py.datanode.exception.InappropriateLogStatusException;
import py.datanode.exception.InsufficientFreeSpaceException;
import py.datanode.exception.InvalidSegmentStatusException;
import py.datanode.exception.LogIdNotFoundException;
import py.datanode.exception.LogIdTooLarge;
import py.datanode.exception.LogIdTooSmall;
import py.datanode.exception.LogNotFoundException;
import py.datanode.exception.ProposalNumberTooSmallException;
import py.datanode.exception.StaleMembershipException;
import py.datanode.page.Page;
import py.datanode.page.PageContext;
import py.datanode.page.PageManager;
import py.datanode.page.impl.GarbagePageAddress;
import py.datanode.page.impl.PageManagerImpl;
import py.datanode.segment.SegmentUnit;
import py.datanode.segment.SegmentUnitCanDeletingCheck;
import py.datanode.segment.SegmentUnitManager;
import py.datanode.segment.copy.PrimaryCopyPageManager;
import py.datanode.segment.copy.PrimaryCopyPageManagerImpl;
import py.datanode.segment.copy.task.PrimaryCopyPageTask;
import py.datanode.segment.datalog.LogImage;
import py.datanode.segment.datalog.MutationLogEntry;
import py.datanode.segment.datalog.MutationLogEntry.LogStatus;
import py.datanode.segment.datalog.MutationLogEntryFactory;
import py.datanode.segment.datalog.MutationLogEntrySaveProxy;
import py.datanode.segment.datalog.MutationLogManager;
import py.datanode.segment.datalog.PrimaryLogCollection;
import py.datanode.segment.datalog.SegmentLogMetadata;
import py.datanode.segment.datalog.SegmentLogMetadata.PeerStatus;
import py.datanode.segment.datalog.persist.LogPersister;
import py.datanode.segment.datalog.persist.LogStorageReader;
import py.datanode.segment.datalog.persist.full.TempLogPersister;
import py.datanode.segment.datalog.plal.engine.PlalEngine;
import py.datanode.segment.datalog.sync.log.SyncLogTaskExecutor;
import py.datanode.segment.datalog.sync.log.reduce.SyncLogReduceCollector;
import py.datanode.segment.datalogbak.catchup.CatchupLogContextKey;
import py.datanode.segment.datalogbak.catchup.CatchupLogContextKey.CatchupLogDriverType;
import py.datanode.segment.heartbeat.HostHeartbeat;
import py.datanode.segment.membership.statemachine.processors.MigrationUtils;
import py.datanode.service.io.DataNodeIoServiceImpl;
import py.datanode.service.io.SegmentUnitMetadataWriteTask;
import py.datanode.service.io.throttle.IoThrottleManager;
import py.datanode.service.worker.MembershipPusher;
import py.datanode.service.worker.PerformanceTester;
import py.datanode.statistic.AlarmReporter;
import py.debug.DynamicParamConfig;
import py.engine.TaskEngine;
import py.exception.AllSegmentUnitDeadInArchviveException;
import py.exception.ArchiveNotExistException;
import py.exception.ArchiveStatusException;
import py.exception.ChecksumMismatchedException;
import py.exception.FailedToSendBroadcastRequestsException;
import py.exception.GenericThriftClientFactoryException;
import py.exception.LeaseExtensionFrozenException;
import py.exception.NoAvailableBufferException;
import py.exception.QuorumNotFoundException;
import py.exception.SegmentUnitBecomeBrokenException;
import py.exception.SnapshotVersionMissMatchForMergeLogsException;
import py.exception.StorageException;
import py.icshare.BackupDbReporter;
import py.instance.Group;
import py.instance.Instance;
import py.instance.InstanceDomain;
import py.instance.InstanceId;
import py.instance.InstanceStore;
import py.instance.PortType;
import py.membership.SegmentMembership;
import py.membership.SegmentMembershipHelper;
import py.netty.client.GenericAsyncClientFactory;
import py.netty.client.TransferenceClientOption;
import py.netty.core.AbstractMethodCallback;
import py.netty.core.IoEventThreadsMode;
import py.netty.core.TransferenceConfiguration;
import py.netty.datanode.AsyncDataNode;
import py.netty.datanode.PyReadResponse;
import py.netty.datanode.PyWriteRequest;
import py.netty.memory.PooledByteBufAllocatorWrapper;
import py.netty.message.ProtocolBufProtocolFactory;
import py.proto.Broadcastlog;
import py.proto.Broadcastlog.PbAsyncSyncLogBatchUnit;
import py.proto.Broadcastlog.PbAsyncSyncLogsBatchRequest;
import py.proto.Broadcastlog.PbBackwardSyncLogRequestUnit;
import py.proto.Broadcastlog.PbBackwardSyncLogResponseUnit;
import py.proto.Broadcastlog.PbBackwardSyncLogsRequest;
import py.proto.Broadcastlog.PbBackwardSyncLogsResponse;
import py.proto.Broadcastlog.PbMembership;
import py.proto.Broadcastlog.PbReadRequest;
import py.proto.Broadcastlog.PbWriteResponse;
import py.storage.Storage;
import py.storage.StorageExceptionHandler;
import py.storage.StorageExceptionHandlerChain;
import py.storage.impl.AsyncStorage;
import py.storage.impl.AsynchronousFileChannelStorageFactory;
import py.storage.impl.PriorityStorageImpl;
import py.third.rocksdb.KvStoreException;
import py.thrift.datanode.service.AbortMutationLogExceptionThrift;
import py.thrift.datanode.service.AbortedLogThrift;
import py.thrift.datanode.service.ArbiterPokePrimaryRequest;
import py.thrift.datanode.service.ArbiterPokePrimaryResponse;
import py.thrift.datanode.service.BackupDatabaseInfoRequest;
import py.thrift.datanode.service.BackupDatabaseInfoResponse;
import py.thrift.datanode.service.BroadcastRequest;
import py.thrift.datanode.service.BroadcastResponse;
import py.thrift.datanode.service.BroadcastTypeThrift;
import py.thrift.datanode.service.CanYouBecomePrimaryRequest;
import py.thrift.datanode.service.CanYouBecomePrimaryResponse;
import py.thrift.datanode.service.CheckPrimaryReachableResponse;
import py.thrift.datanode.service.CheckSecondaryHasAllLogRequest;
import py.thrift.datanode.service.CheckSecondaryHasAllLogResponse;
import py.thrift.datanode.service.CheckSecondaryReachableRequest;
import py.thrift.datanode.service.CheckSecondaryReachableResponse;
import py.thrift.datanode.service.ConfirmPrimaryUnreachableRequest;
import py.thrift.datanode.service.ConfirmPrimaryUnreachableResponse;
import py.thrift.datanode.service.CreateSegmentUnitBatchRequest;
import py.thrift.datanode.service.CreateSegmentUnitBatchResponse;
import py.thrift.datanode.service.CreateSegmentUnitFailedCodeThrift;
import py.thrift.datanode.service.CreateSegmentUnitFailedNode;
import py.thrift.datanode.service.CreateSegmentUnitNode;
import py.thrift.datanode.service.CreateSegmentUnitRequest;
import py.thrift.datanode.service.CreateSegmentUnitResponse;
import py.thrift.datanode.service.DataNodeService;
import py.thrift.datanode.service.DeleteSegmentRequest;
import py.thrift.datanode.service.DeleteSegmentResponse;
import py.thrift.datanode.service.DeleteSegmentUnitRequest;
import py.thrift.datanode.service.DeleteSegmentUnitResponse;
import py.thrift.datanode.service.DepartFromMembershipRequest;
import py.thrift.datanode.service.DepartFromMembershipRespone;
import py.thrift.datanode.service.FailedToBroadcastExceptionThrift;
import py.thrift.datanode.service.FailedToKickOffPrimaryExceptionThrift;
import py.thrift.datanode.service.HeartbeatRequest;
import py.thrift.datanode.service.HeartbeatRequestUnit;
import py.thrift.datanode.service.HeartbeatResponse;
import py.thrift.datanode.service.HeartbeatResponseUnit;
import py.thrift.datanode.service.HeartbeatResultThrift;
import py.thrift.datanode.service.ImReadyToBePrimaryRequest;
import py.thrift.datanode.service.ImReadyToBePrimaryResponse;
import py.thrift.datanode.service.ImReadyToBeSecondaryRequest;
import py.thrift.datanode.service.ImReadyToBeSecondaryResponse;
import py.thrift.datanode.service.InappropriateLogStatusExceptionThrift;
import py.thrift.datanode.service.InitializeBitmapExceptionThrift;
import py.thrift.datanode.service.InitiateCopyPageRequestThrift;
import py.thrift.datanode.service.InitiateCopyPageResponseThrift;
import py.thrift.datanode.service.InnerMigrateSegmentUnitRequest;
import py.thrift.datanode.service.InnerMigrateSegmentUnitResponse;
import py.thrift.datanode.service.InputHasNoDataExceptionThrift;
import py.thrift.datanode.service.InvalidSegmentUnitStatusExceptionThrift;
import py.thrift.datanode.service.InvalidTokenThrift;
import py.thrift.datanode.service.InvalidateCacheRequest;
import py.thrift.datanode.service.InvalidateCacheResponse;
import py.thrift.datanode.service.JoiningGroupRequest;
import py.thrift.datanode.service.JoiningGroupRequestDeniedExceptionThrift;
import py.thrift.datanode.service.JoiningGroupResponse;
import py.thrift.datanode.service.KickOffPotentialPrimaryRequest;
import py.thrift.datanode.service.KickOffPotentialPrimaryResponse;
import py.thrift.datanode.service.LeaseExtensionFrozenExceptionThrift;
import py.thrift.datanode.service.LogStatusThrift;
import py.thrift.datanode.service.LogSyncActionThrift;
import py.thrift.datanode.service.LogThrift;
import py.thrift.datanode.service.MakePrimaryDecisionRequest;
import py.thrift.datanode.service.MakePrimaryDecisionResponse;
import py.thrift.datanode.service.MigratePrimaryRequest;
import py.thrift.datanode.service.MigratePrimaryResponse;
import py.thrift.datanode.service.NoAvailableCandidatesForNewPrimaryExceptionThrift;
import py.thrift.datanode.service.NoOneCanBeReplacedExceptionThrift;
import py.thrift.datanode.service.NotSupportedExceptionThrift;
import py.thrift.datanode.service.OnSameArchiveExceptionThrift;
import py.thrift.datanode.service.PrimaryIsRollingBackExceptionThrift;
import py.thrift.datanode.service.PrimaryNeedRollBackFirstExceptionThrift;
import py.thrift.datanode.service.ProgressThrift;
import py.thrift.datanode.service.ReadRequest;
import py.thrift.datanode.service.ReadResponse;
import py.thrift.datanode.service.RefuseDepartFromMembershipExceptionThrift;
import py.thrift.datanode.service.ReleaseAllLogsRequest;
import py.thrift.datanode.service.ReleaseAllLogsResponse;
import py.thrift.datanode.service.RetrieveBitMapRequest;
import py.thrift.datanode.service.RetrieveBitMapResponse;
import py.thrift.datanode.service.SecondaryCopyPagesRequest;
import py.thrift.datanode.service.SecondaryCopyPagesResponse;
import py.thrift.datanode.service.SecondarySnapshotVersionTooHighExceptionThrift;
import py.thrift.datanode.service.SegmentNotFoundExceptionThrift;
import py.thrift.datanode.service.StartOnlineMigrationRequest;
import py.thrift.datanode.service.StartOnlineMigrationResponse;
import py.thrift.datanode.service.SyncLogsRequest;
import py.thrift.datanode.service.SyncLogsResponse;
import py.thrift.datanode.service.UpdateSegmentUnitMembershipAndStatusRequest;
import py.thrift.datanode.service.UpdateSegmentUnitMembershipAndStatusResponse;
import py.thrift.datanode.service.UpdateSegmentUnitVolumeMetadataJsonRequest;
import py.thrift.datanode.service.UpdateSegmentUnitVolumeMetadataJsonResponse;
import py.thrift.datanode.service.WriteMutationLogExceptionThrift;
import py.thrift.datanode.service.WriteRequest;
import py.thrift.datanode.service.WriteResponse;
import py.thrift.datanode.service.WrongPrePrimarySessionExceptionThrift;
import py.thrift.share.ArchiveIsUsingExceptionThrift;
import py.thrift.share.ArchiveManagerNotSupportExceptionThrift;
import py.thrift.share.ArchiveNotFoundExceptionThrift;
import py.thrift.share.ArchiveTypeNotSupportExceptionThrift;
import py.thrift.share.ArchiveTypeThrift;
import py.thrift.share.BroadcastLogThrift;
import py.thrift.share.ChecksumMismatchedExceptionThrift;
import py.thrift.share.CommitLogsRequestThrift;
import py.thrift.share.CommitLogsResponseThrift;
import py.thrift.share.ConfirmAbortLogsRequestThrift;
import py.thrift.share.ConfirmAbortLogsResponseThrift;
import py.thrift.share.ConnectionRefusedExceptionThrift;
import py.thrift.share.DatanodeIsUsingExceptionThrift;
import py.thrift.share.DegradeDiskRequest;
import py.thrift.share.DegradeDiskResponse;
import py.thrift.share.DiskHasBeenOfflineThrift;
import py.thrift.share.DiskHasBeenOnlineThrift;
import py.thrift.share.DiskIsBusyThrift;
import py.thrift.share.DiskNotBrokenThrift;
import py.thrift.share.DiskNotFoundExceptionThrift;
import py.thrift.share.DiskNotMismatchConfigThrift;
import py.thrift.share.DiskSizeCanNotSupportArchiveTypesThrift;
import py.thrift.share.ForceUpdateMembershipRequest;
import py.thrift.share.ForceUpdateMembershipResponse;
import py.thrift.share.FreeArchiveStoragePoolRequestThrift;
import py.thrift.share.FreeArchiveStoragePoolResponseThrift;
import py.thrift.share.FreeDatanodeDomainRequestThrift;
import py.thrift.share.FreeDatanodeDomainResponseThrift;
import py.thrift.share.GetConfigurationsRequest;
import py.thrift.share.GetConfigurationsResponse;
import py.thrift.share.GetDbInfoRequestThrift;
import py.thrift.share.GetDbInfoResponseThrift;
import py.thrift.share.GetLatestLogsRequestThrift;
import py.thrift.share.GetLatestLogsResponseThrift;
import py.thrift.share.HeartbeatDisableRequest;
import py.thrift.share.HeartbeatDisableResponse;
import py.thrift.share.InternalErrorThrift;
import py.thrift.share.InvalidInputExceptionThrift;
import py.thrift.share.InvalidMembershipExceptionThrift;
import py.thrift.share.InvalidParameterValueExceptionThrift;
import py.thrift.share.LeaseExpiredExceptionThrift;
import py.thrift.share.LogIdTooSmallExceptionThrift;
import py.thrift.share.NoMemberExceptionThrift;
import py.thrift.share.NotEnoughSpaceExceptionThrift;
import py.thrift.share.NotPrimaryExceptionThrift;
import py.thrift.share.NotSecondaryExceptionThrift;
import py.thrift.share.OfflineDiskRequest;
import py.thrift.share.OfflineDiskResponse;
import py.thrift.share.OnlineDiskRequest;
import py.thrift.share.OnlineDiskResponse;
import py.thrift.share.PrimaryCandidateCantBePrimaryExceptionThrift;
import py.thrift.share.PrimaryExistsExceptionThrift;
import py.thrift.share.ReceiverStaleMembershipExceptionThrift;
import py.thrift.share.RemoveUncommittedLogsExceptForThoseGivenRequest;
import py.thrift.share.RemoveUncommittedLogsExceptForThoseGivenResponse;
import py.thrift.share.ReportDbRequestThrift;
import py.thrift.share.ResourceExhaustedExceptionThrift;
import py.thrift.share.SegIdThrift;
import py.thrift.share.SegmentExistingExceptionThrift;
import py.thrift.share.SegmentMembershipThrift;
import py.thrift.share.SegmentOfflinedExceptionThrift;
import py.thrift.share.SegmentUnitBeingDeletedExceptionThrift;
import py.thrift.share.SegmentUnitCloningExceptionThrift;
import py.thrift.share.SegmentUnitStatusThrift;
import py.thrift.share.SegmentUnitTypeThrift;
import py.thrift.share.ServiceHavingBeenShutdownThrift;
import py.thrift.share.SetArchiveConfigRequest;
import py.thrift.share.SetArchiveConfigResponse;
import py.thrift.share.SetArchiveStoragePoolRequestThrift;
import py.thrift.share.SetArchiveStoragePoolResponseThrift;
import py.thrift.share.SetConfigurationsRequest;
import py.thrift.share.SetConfigurationsResponse;
import py.thrift.share.SetDatanodeDomainRequestThrift;
import py.thrift.share.SetDatanodeDomainResponseThrift;
import py.thrift.share.SetIoErrorRequest;
import py.thrift.share.SetIoErrorResponse;
import py.thrift.share.SettleArchiveTypeRequest;
import py.thrift.share.SettleArchiveTypeResponse;
import py.thrift.share.SnapshotRollingBackExceptionThrift;
import py.thrift.share.StaleMembershipExceptionThrift;
import py.thrift.share.VolumeSourceThrift;
import py.thrift.share.VolumeTypeThrift;
import py.thrift.share.WriteMutationLogsDisableRequest;
import py.thrift.share.WriteMutationLogsDisableResponse;
import py.thrift.share.YouAreNotInMembershipExceptionThrift;
import py.thrift.share.YouAreNotInRightPositionExceptionThrift;
import py.thrift.share.YouAreNotReadyExceptionThrift;
import py.volume.VolumeMetadata;
import py.volume.VolumeType;

/**
 * Implement blocking interface of DataNodeService.
 *
 * <p>cases where they can't.
 *
 * <p>instead of creating a new one within this class.
 */
public class DataNodeServiceImpl implements DataNodeService.Iface,
    ThriftProcessorFactory {

  private static final Logger logger = LoggerFactory.getLogger(DataNodeServiceImpl.class);
  private static final String ENABLE_LOGGER_TRACER = "enableLoggerTracer";
  private static final String SWITCH_L2_CACHE = "switch.level.two.cache";
  private static final String MAX_MIGRATION_SPEED = "max.migration.speed";
  private static final long LOCK_STATUS_TIMEOUT_MS = 1000;

  private final Map<SegId, Long> currentCreatingSegmentUnit = new ConcurrentHashMap<>();

  // data node service client for accessing other data node service if I am a
  // primary
  private DataNodeServiceAsyncClientWrapper dataNodeAsyncClient = null;
  private GenericThriftClientFactory<DataNodeService.Iface> dataNodeSyncClientFactory = null;

  /* Member variables that need to be set */
  private AppContext context;
  private SegmentUnitManager segmentUnitManager;
  private PageManager<Page> pageManager;
  private MutationLogManager mutationLogManager;
  private DataNodeConfiguration cfg;
  private RawArchiveManager archiveManager;
  private InstanceStore instanceStore;
  private LogStorageReader logStorageReader;
  private LogPersister logPersister;
  private SegmentUnitTaskExecutor catchupLogEngine;
  private SegmentUnitTaskExecutor stateProcessingEngine;
  private StorageExceptionHandler storageExceptionHandler;
  private HostHeartbeat hostHeartbeat;
  private UnsettledArchiveManager unsettledArchiveManager;
  private PlalEngine plalEngine;
  private TempLogPersister tempLogPersister;
  private TaskEngine copyPageTaskEngine;

  // The reason of not using AtomicBoolean is to save the cost of thread
  // synchronization.
  private boolean shutdown = false;
  private boolean shutdownCompletely = false;
  private boolean pauseRequestProcessing = false;
  private StorageExceptionHandlerChain storageExceptionHandlerChain;
  private DataNodeService.Processor<DataNodeService.Iface> processor;
  private Random randomForJitter = new Random(System.currentTimeMillis());
  private GenericThriftClientFactory<DataNodeService.AsyncIface> dataNodeAsyncClientFactory = null;

  // used for shutdown the engine
  private ThriftAppEngine appEngine;
  private AtomicBoolean haveInvokedShutdownApi = new AtomicBoolean(false);
  private MutationLogEntrySaveProxy saveLogProxy;
  private BackupDbReporter backupDbReporter;
  private SegmentUnitCanDeletingCheck segmentUnitCanDeletingCheck;
  private HashedWheelTimer hashedWheelTimer = null;
  private boolean arbiterDatanode = false;
  private DataNodeIoServiceImpl ioService;
  private GenericAsyncClientFactory<AsyncDataNode.AsyncIface> ioClientFactory;
  private ByteBufAllocator byteBufAllocator = PooledByteBufAllocatorWrapper.INSTANCE;
  private PluginPlugoutManager pluginPlugoutManager;
  private IoThrottleManager ioThrottleManager;
  private AlarmReporter alarmReporter;
  private ConsumerService<SegmentUnitMetadataWriteTask> persistSegmentUnitEngine;

  // for sync log message reduce collector
  private SyncLogReduceCollector<PbBackwardSyncLogRequestUnit,
      PbBackwardSyncLogsRequest, PbBackwardSyncLogsRequest> backwardSyncLogRequestReduceCollector;
  private SyncLogReduceCollector<PbBackwardSyncLogResponseUnit,
      PbBackwardSyncLogsResponse, PbBackwardSyncLogsRequest> backwardSyncLogResponseReduceCollector;
  private SyncLogReduceCollector<PbAsyncSyncLogBatchUnit,
      PbAsyncSyncLogsBatchRequest, PbAsyncSyncLogsBatchRequest> syncLogBatchRequestReduceCollector;

  // for sync log task executor
  private SyncLogTaskExecutor syncLogTaskExecutor;

  private Executor becomePrimaryExecutor;

  public enum BecomePrimaryPriorityBonus {
    // temp primary participated, we need a new epoch ASAP
    TEMP_PRIMARY(10),
    // no primary can be voted out with anyone down
    JUST_ENOUGH_MEMBERS(5),
    // no primary can be voted out with the current candidate down
    ONLY_ONE_CANDIDATE(4),
    // there is only one data backup (not arbiter) left
    DATA_BACKUP_COUNT_1(3),
    // there are only two data backups (not arbiter) left
    DATA_BACKUP_COUNT_2(1),
    // not much data copy during the process of becoming primary
    NOT_MUCH_DATA_COPY(1);

    private int value;

    BecomePrimaryPriorityBonus(int value) {
      this.value = value;
    }

    public int getValue() {
      return value;
    }
  }

  /**
   * end for sync log task member.
   **/
  public DataNodeServiceImpl() {
    processor = new DataNodeService.Processor<DataNodeService.Iface>(this);
    ioService = new DataNodeIoServiceImpl(this);

    persistSegmentUnitEngine =
        new ConsumerServiceDispatcher<RawArchive, SegmentUnitMetadataWriteTask>(
            ioTask -> ioTask.getSegmentUnit().getArchive(), rawArchive -> {
          SingleThreadConsumerService<SegmentUnitMetadataWriteTask> consumerService =
              new SingleThreadConsumerService<>(
                  SegmentUnitMetadataWriteTask::run, new PriorityBlockingQueue<>(),
                  Storage.getDeviceName("write-meta-engine-"
                      + rawArchive.getArchiveMetadata().getDeviceName()));
          return consumerService;
        });

  }

  @Override
  public void shutdown() throws org.apache.thrift.TException {
    logger.warn("starting shutdown service for instance={}", getContext().getInstanceId());
    if (haveInvokedShutdownApi.compareAndSet(false, true)) {
      shutdown = true;
      ioService.shutdown();

      Thread shutdownThread = new Thread(new Runnable() {
        @Override
        public void run() {
          if (appEngine != null) {
            logger.info("data node engine is being shutdown");
            try {
              appEngine.stop();
            } catch (Throwable t) {
              logger.error("caught an exception", t);
            }
          }
        }
      });
      shutdownThread.start();
    }
  }

  public void stop() {
    shutdown = true;
    ioService.shutdown();
    Exception closeStackTrace = new Exception("Capture service shutdown signal");
    logger.warn("shutting down data node service. stack trace of this: ", closeStackTrace);

    for (SegmentUnit unit : segmentUnitManager.get()) {
      unit.setDataNodeServiceShutdown(true);
    }

    try {
      ioClientFactory.close();
    } catch (Throwable t) {
      logger.error("caught an exception", t);
    }

    persistSegmentUnitEngine.stop();
    ioService.stopEngine();
    shutdownCompletely = true;
    logger.warn("ending shutdown service for instance={}", getContext().getInstanceId());

  }

  public boolean isShutdownFinished() {
    return shutdownCompletely;
  }

  public void init() throws Exception {
    logger.info("Setting up the service ...");

    TransferenceConfiguration transferenceConfiguration = GenericAsyncClientFactory
        .defaultConfiguration();
    transferenceConfiguration.option(TransferenceClientOption.CONNECTION_COUNT_PER_ENDPOINT, 1);
    transferenceConfiguration.option(TransferenceClientOption.IO_TIMEOUT_MS, 10000);

    transferenceConfiguration.option(TransferenceClientOption.MAX_BYTES_ONCE_ALLOCATE,
        cfg.getNettyConfiguration().getNettyMaxBufLengthForAllocateAdapter());
    transferenceConfiguration.option(TransferenceClientOption.CLIENT_IO_EVENT_GROUP_THREADS_MODE,
        IoEventThreadsMode
            .findByValue(cfg.getNettyConfiguration().getNettyClientIoEventGroupThreadsMode()));
    transferenceConfiguration
        .option(TransferenceClientOption.CLIENT_IO_EVENT_GROUP_THREADS_PARAMETER,
            cfg.getNettyConfiguration().getNettyClientIoEventGroupThreadsParameter());
    transferenceConfiguration.option(TransferenceClientOption.CLIENT_IO_EVENT_HANDLE_THREADS_MODE,
        IoEventThreadsMode
            .findByValue(cfg.getNettyConfiguration().getNettyClientIoEventHandleThreadsMode()));
    transferenceConfiguration
        .option(TransferenceClientOption.CLIENT_IO_EVENT_HANDLE_THREADS_PARAMETER,
            cfg.getNettyConfiguration().getNettyClientIoEventHandleThreadsParameter());

    ProtocolBufProtocolFactory protocolFactory =
        (ProtocolBufProtocolFactory) ProtocolBufProtocolFactory
            .create(AsyncDataNode.AsyncIface.class);
    ioClientFactory = new GenericAsyncClientFactory<AsyncDataNode.AsyncIface>(
        AsyncDataNode.AsyncIface.class,
        protocolFactory, transferenceConfiguration);
    ioClientFactory.setAllocator(byteBufAllocator);
    ioClientFactory.init();

    ioService.setCfg(cfg);
    ioService.setContext(context);
    ioService.setPageManager(pageManager);
    ioService.setPlalEngine(plalEngine);
    ioService.setSegmentUnitManager(segmentUnitManager);
    ioService.setTempLogPersister(tempLogPersister);
    ioService.setDataNodeAsyncClient(dataNodeAsyncClient);
    ioService.setInstanceStore(instanceStore);

    ioService.setBackwardSyncLogRequestReduceCollector(backwardSyncLogRequestReduceCollector);
    ioService.setBackwardSyncLogResponseReduceCollector(backwardSyncLogResponseReduceCollector);
    ioService.setSyncLogBatchRequestReduceCollector(syncLogBatchRequestReduceCollector);
    ioService.setSyncLogTaskExecutor(syncLogTaskExecutor);

    ioService.setByteBufAllocator(PooledByteBufAllocatorWrapper.INSTANCE);
    ioService.setArchiveManager(archiveManager);
    ioService.setAsyncClientFactory(ioClientFactory);
    ioService.setIoThrottleManager(ioThrottleManager);
    ioService.setAlarmReporter(alarmReporter);
    ioService.init();

    initSegmentUnit();
    persistSegmentUnitEngine.start();

    AtomicLong threadIterator = new AtomicLong(0);
    ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(
        cfg.getBecomePrimaryThreadPoolSize(),
        cfg.getBecomePrimaryThreadPoolSize(),
        60L, TimeUnit.SECONDS, new PriorityBlockingQueue<>(),
        r -> new Thread(r, "BecomePrimary-" + threadIterator.incrementAndGet()));

    threadPoolExecutor.allowCoreThreadTimeOut(true);
    becomePrimaryExecutor = threadPoolExecutor;
  }

  private void initSegmentUnit() throws Exception {
    for (SegmentUnit unit : segmentUnitManager.get()) {
      SegId segId = unit.getSegId();
      logger.warn("Working on segment unit {} ", segId);
      SegmentUnitStatus status = unit.getSegmentUnitMetadata().getStatus();
      logger.warn("The status is {} ", status);

      if (status != SegmentUnitStatus.Deleted && !unit.isArbiter()) {
        logger.info("Initiating segment unit log metedata ...");
        SegmentLogMetadata logMetadata = initSegmentLogMetadata(segId,
            unit.getSegmentUnitMetadata().getVolumeType(),
            mutationLogManager, logStorageReader, unit.getArchive().getArchiveId());

        unit.setSegmentLogMetadata(logMetadata);
        logMetadata.setSegmentUnit(unit);
        unit.setPlalEngine(plalEngine);

        for (MutationLogEntry log : logMetadata.getLogsAfterPcl()) {
          if (log.getStatus() == LogStatus.Committed) {
            int pageIndex = (int) (log.getOffset() / cfg.getPageSize());
            unit.setPageHasBeenWrittenAndFirstWritePageIndex(pageIndex);
          }
        }
      }
    }
  }

  private SegmentLogMetadata initSegmentLogMetadata(SegId segId, VolumeType volumeType,
      MutationLogManager mutationLogManager, LogStorageReader logStorageReader,
      Long archiveId)
      throws IOException, LogNotFoundException, LogIdTooSmall, LogIdTooLarge,
      InappropriateLogStatusException,
      KvStoreException {
    SegmentLogMetadata logMetadata = mutationLogManager
        .createOrGetSegmentLogMetadata(segId, volumeType.getWriteQuorumSize());

    List<MutationLogEntry> logs = null;
    try {
      logs = logStorageReader.readLatestLogs(segId, 100);
    } catch (KvStoreException e) {
      logger.info("", e);
      logs = Lists.newArrayList();
    }

    long lastLogId = LogImage.INVALID_LOG_ID;
    long minLogId = lastLogId;

    if (logs.size() > 0) {
      logger.warn("read the last 100 logs in the log storage system, size {}, lastLogId {}",
          logs.size(),
          logs.get(logs.size() - 1));

      for (MutationLogEntry log : logs) {
        if (log.getLogId() == LogImage.INVALID_LOG_ID) {
          continue;
        }
        logMetadata.appendLog(log);
        lastLogId = log.getLogId();
        minLogId = Math.min(log.getLogId(), minLogId);
      }
    }

    List<MutationLogEntry> loadSaveLogs = saveLogProxy.loadLogs(segId, archiveId);
    long logExpirationTime = System.currentTimeMillis() + cfg.getIoTimeoutMs();
    if (loadSaveLogs != null && !loadSaveLogs.isEmpty()) {
      for (MutationLogEntry saveLog : loadSaveLogs) {
        saveLog.setExpirationTimeMs(logExpirationTime);
        if (saveLog.getLogId() == 0) {
          logger.warn("it is strange for log={}, because log id equals 0", saveLog);
          logMetadata.saveLogsWithoutLogId(Lists.newArrayList(saveLog), null);
        } else {
          if (saveLog.isApplied()) {
            logger.warn("the log={} has been applied", saveLog);
            if (!saveLog.isPersisted() && saveLog.getStatus() != LogStatus.Aborted) {
              saveLog.setPersisted();
            }
          } else {
            if (saveLog.getData() == null) {
              logger.warn("the log={} is not applied, but the data is null", saveLog);
            }
          }
          logMetadata.appendLog(saveLog);
          minLogId = Math.min(saveLog.getLogId(), minLogId);
        }
      }
      saveLogProxy.deleteFileBySegId(segId);
    }

    logMetadata.moveClTo(lastLogId);
    logMetadata.moveLalTo(lastLogId);
    logMetadata.movePlTo(lastLogId);
    logMetadata.setSwplIdTo(minLogId);

    LogImage image = logMetadata.getLogImageIncludeLogsAfterPcl();
    logger.warn("last log ID {} and logImage is {}", lastLogId, image);

    return logMetadata;
  }

  @Override
  public void ping() throws org.apache.thrift.TException {
    if (shutdown) {
      throw new ServiceHavingBeenShutdownThrift();
    }
    if (pauseRequestProcessing) {
      throw new ConnectionRefusedExceptionThrift();
    }

  }

  @Override
  public JoiningGroupResponse canJoinGroup(JoiningGroupRequest request)
      throws SegmentNotFoundExceptionThrift, NotPrimaryExceptionThrift, TException,
      InvalidMembershipExceptionThrift, JoiningGroupRequestDeniedExceptionThrift,
      ServiceHavingBeenShutdownThrift {
    logger.debug("Joining group request {} ", request);

    if (shutdown) {
      throw new ServiceHavingBeenShutdownThrift();
    }
    if (pauseRequestProcessing) {
      throw new ConnectionRefusedExceptionThrift();
    }

    SegId segId = new SegId(request.getVolumeId(), request.getSegIndex());
    SegmentUnit segUnit = segmentUnitManager.get(segId);

    Validator.validateJoinGroupInput(request, segUnit, context);

    SegmentMembership currentMembership = segUnit.getSegmentUnitMetadata().getMembership();

    InstanceId secondaryInstanceId = new InstanceId(request.getMyself());
    SegmentMembership newMembership = segUnit
        .addSecondary(secondaryInstanceId, request.isArbiter(), cfg.getSecondaryLeaseInPrimaryMs(),
            request.isSecondaryCandidate());
    if (newMembership == null) {
      if (MigrationUtils
          .explicitlyDenyNewSecondary(segUnit.getSegmentUnitMetadata().getMembership(),
              secondaryInstanceId, request.isArbiter(),
              segUnit.getSegmentUnitMetadata().getVolumeType())) {
        String errMsg = "Denied the request of the applicant "
            + "who wants to join the group because there is no available slot";
        logger.info(errMsg);
        logger.warn("denying a secondary from joining, "
                + "this may cause the secondary to delete itself!!, {} {} {}",
            segUnit, secondaryInstanceId, request.isArbiter());
        JoiningGroupRequestDeniedExceptionThrift e = new JoiningGroupRequestDeniedExceptionThrift();
        e.setDetail(errMsg);
        throw e;
      } else {
        throw new InternalErrorThrift().setDetail("deny the request, "
            + "but we don't want the request secondary to delete itself");
      }
    } else if (newMembership.equals(currentMembership)) {
      logger.debug("member {} is already in the membership", secondaryInstanceId);
      JoiningGroupResponse joiningGroupResponse = new JoiningGroupResponse(request.getRequestId(),
          DataNodeRequestResponseHelper.buildThriftMembershipFrom(segId, newMembership),
          mutationLogManager.getSegment(segId).getMaxLogId());
      return joiningGroupResponse;
    }

    logger
        .warn("{} Allow {} to join the group. The new membership is {}", segId, request.getMyself(),
            newMembership);

    try {
      archiveManager.updateSegmentUnitMetadata(segId, newMembership, null);
      MembershipPusher.getInstance(context.getInstanceId()).submit(segId);
    } catch (Exception e1) {
      SegmentMembership latestMembership = segUnit.getSegmentUnitMetadata().getMembership();

      String errMsg = "Failed to update to the new membership for segment " + segId;
      logger.warn(errMsg, e1);

      segUnit.clearPeerLeases();

      segUnit.startPeerLeases(cfg.getSecondaryLeaseInPrimaryMs(),
          latestMembership.getAliveSecondaries());
      segUnit.clearExpiredMembers();

      for (InstanceId inactiveSecondary : latestMembership.getInactiveSecondaries()) {
        segUnit.startPeerLease(Integer.MIN_VALUE, inactiveSecondary);
      }

      InternalErrorThrift e = new InternalErrorThrift();
      e.setDetail(errMsg);
      throw e;
    }

    SegmentLogMetadata segmentLogMetadata = mutationLogManager.getSegment(segId);
    long newSwplId = segmentLogMetadata.getSwplId();
    long newSwclId = segmentLogMetadata.getSwclId();
    if (!segUnit.isArbiter()) {
      PeerStatus peerStatus = PeerStatus.getPeerStatusInMembership(
          segUnit.getSegmentUnitMetadata().getMembership(), secondaryInstanceId);

      newSwplId = segmentLogMetadata
          .peerUpdatePlId(request.getPpl(), secondaryInstanceId,
              cfg.getThresholdToClearSecondaryPlMs(), peerStatus);
      segmentLogMetadata.setSwplIdTo(newSwplId);

      newSwclId = segmentLogMetadata
          .peerUpdateClId(request.getPcl(), secondaryInstanceId,
              cfg.getThresholdToClearSecondaryClMs(), peerStatus);
      segmentLogMetadata.setSwclIdTo(newSwclId);
    }

    if (logger.isDebugEnabled()) {
      logger.debug("Broadcast JoiningGroup messages to current secondaries: {}",
          Joiner.on("\t").join(currentMembership.getAliveSecondaries()));
    }
    BroadcastRequest joiningGroup = DataNodeRequestResponseHelper
        .buildAddOrRemoveMemberRequest(request.getRequestId(), segId, newMembership, newSwplId,
            newSwclId);

    try {
      List<EndPoint> endpoints = DataNodeRequestResponseHelper
          .buildEndPoints(instanceStore, newMembership, true, context.getInstanceId(),
              secondaryInstanceId);

      if (endpoints.size() > 0) {
        dataNodeAsyncClient
            .broadcast(segUnit, cfg.getSecondaryLeaseInPrimaryMs(), joiningGroup, 1000,
                0, true, endpoints);
        logger.debug("Successfully broadcast joiningGroup request to secondaries.");
      }
    } catch (Exception e) {
      logger
          .info("Caught an exception when broadcasting joiningGroup to all secendaries. Ignore it",
              e);
    }

    segUnit.getSegmentUnitMetadata().clearRollBackingJs();

    long sfal = segmentLogMetadata.getMaxLogId();
    JoiningGroupResponse joiningGroupResponse = new JoiningGroupResponse(request.getRequestId(),
        DataNodeRequestResponseHelper.buildThriftMembershipFrom(segId, newMembership), sfal);
    return joiningGroupResponse;
  }

  @Override
  public CreateSegmentUnitBatchResponse createSegmentUnitBatch(
      CreateSegmentUnitBatchRequest request)
      throws NotEnoughSpaceExceptionThrift, SegmentExistingExceptionThrift,
      SegmentUnitBeingDeletedExceptionThrift, NoMemberExceptionThrift,
      ServiceHavingBeenShutdownThrift,
      InternalErrorThrift, SegmentOfflinedExceptionThrift, TException {
    logger.warn("create segment unit batch request :{} ", request);

    if (shutdown) {
      throw new ServiceHavingBeenShutdownThrift();
    }
    if (pauseRequestProcessing) {
      throw new ConnectionRefusedExceptionThrift();
    }

    long volumeId = request.getVolumeId();
    VolumeTypeThrift volumeTypeThrift = request.getVolumeType();
    String volumeMetadataJson = request.getVolumeMetadataJson();
    String accountMetadataJson = request.getAccountMetadataJson();
    long storagePoolId = request.getStoragePoolId();
    boolean enableLaunchMultiDrivers = request.isEnableLaunchMultiDrivers();

    VolumeType volumeType = DataNodeRequestResponseHelper.convertVolumeType(volumeTypeThrift);
    List<SegmentUnit> succeedSegmentUnits = Collections.synchronizedList(new ArrayList<>());
    List<Integer> succeedSegIndexes = new ArrayList<>();
    List<CreateSegmentUnitFailedNode> failedSegIndexes = Collections
        .synchronizedList(new ArrayList<>());
    Map<Long, List<SegmentUnit>> archiveToSegmentUnits = new HashMap<>();
    List<CreateSegmentUnitNode> createSegmentUnitNodes = request.getSegmentUnits();
    int waitPersistSegmentUnits = 0;

    for (CreateSegmentUnitNode createSegmentUnitNode : createSegmentUnitNodes) {
      SegId segId = new SegId(request.getVolumeId(), createSegmentUnitNode.getSegIndex());
      Long oldRequestId = null;
      if (null != (oldRequestId = currentCreatingSegmentUnit
          .putIfAbsent(segId, request.getRequestId()))) {
        logger.error("found a creating segment unit received by CreateSegmentUnits"
                + " creating request id {}, segment unit like that :{}", oldRequestId,
            createSegmentUnitNode);
        failedSegIndexes.add(new CreateSegmentUnitFailedNode(createSegmentUnitNode.getSegIndex(),
            CreateSegmentUnitFailedCodeThrift.SegmentExistingException));
        continue;
      }
      try {
        SegmentMembershipThrift initMembershipThrift = createSegmentUnitNode.getInitMembership();
        SegmentMembership initMembership = null;
        if (initMembershipThrift == null) {
          List<Long> initMembers = createSegmentUnitNode.getInitMembers();
          if (initMembers == null || initMembers.isEmpty()) {
            throw new NoMemberExceptionThrift();
          }

          initMembership = new SegmentMembership(new InstanceId(initMembers.get(0)),
              DataNodeRequestResponseHelper.convertFromLong(initMembers));
        } else {
          initMembership = DataNodeRequestResponseHelper
              .buildSegmentMembershipFrom(createSegmentUnitNode.getInitMembership()).getSecond();
        }
        logger.debug("create segment unit {} with segment membership: {}", createSegmentUnitNode,
            initMembership);

        VolumeSourceThrift volumeSource = createSegmentUnitNode.getVolumeSource();

        SegmentUnit segUnit = segmentUnitManager.get(segId);
        if (segUnit != null) {
          SegmentUnitMetadata segUnitMetadata = segUnit.getSegmentUnitMetadata();
          logger.warn("existing segment unit {}", segUnitMetadata);
          if (segUnitMetadata.isSegmentUnitMarkedAsDeleting() && !segUnitMetadata
              .getMigrationStatus()
              .isMigratedStatus()) {
            if (createSegmentUnitNode.isSecondaryCandidate()) {
              throw new SegmentUnitBeingDeletedExceptionThrift();
            }

            long beginningOfQuarantineZone =
                segUnitMetadata.getLastUpdated() + cfg.getWaitTimeMsToMoveToDeleted() - cfg
                    .getQuarantineZoneMsToCreateDeletedSegment();

            long currentTime = System.currentTimeMillis();
            logger.debug("The last time of updating segment unit was {} "
                    + ", and current time is {} and the beginning of the "
                    + "quarantineZone is {} and we are {} from quarantine zone",
                segUnitMetadata.getLastUpdated(), currentTime, beginningOfQuarantineZone,
                currentTime - beginningOfQuarantineZone);

            if (currentTime > beginningOfQuarantineZone
                || segUnit.getArchive().getArchiveStatus() == ArchiveStatus.DEGRADED) {
              logger.warn("We are in the quarantine zone. So throw an exception");
              throw new SegmentUnitBeingDeletedExceptionThrift();
            } else {
              try {
                SegmentUnitType requestSegmentUnitType = RequestResponseHelper
                    .buildSegmentUnitTypeFromThrift(createSegmentUnitNode.getSegmentUnitType());
                SegmentUnitType existSegmentUnitType = segUnit.getSegmentUnitMetadata()
                    .getSegmentUnitType();
                if (requestSegmentUnitType.getValue() == existSegmentUnitType.getValue()) {
                  archiveManager
                      .updateSegmentUnitMetadata(segId, initMembership, SegmentUnitStatus.Start);
                } else {
                  logger.warn(
                      "exist segment unit type:{} is not same with request segment unit type:{}",
                      existSegmentUnitType, requestSegmentUnitType);
                  throw new SegmentUnitBeingDeletedExceptionThrift();
                }

                succeedSegIndexes.add(segId.getIndex());
                currentCreatingSegmentUnit.remove(segId);
                continue;
              } catch (Exception e) {
                logger.warn("caught an exception when creating the segment unit {} ", segId, e);
                throw new InternalErrorThrift().setDetail(
                    "Can't create the segment unit because it is at the deleting status "
                        + "and can't be deleted first"
                        + e.getMessage());
              }
            }
          } else if (segUnitMetadata.isSegmentUnitDeleted()) {
            logger.warn(
                "segment unit={} has been deleted, so must wait for the segment to be discarded",
                segUnitMetadata);
            throw new SegmentUnitBeingDeletedExceptionThrift();
          } else if (segUnitMetadata.getStatus() == SegmentUnitStatus.OFFLINED) {
            throw new SegmentOfflinedExceptionThrift().setDetail(segUnitMetadata.toString());
          } else {
            throw new SegmentExistingExceptionThrift()
                .setDetail(segUnitMetadata.getStatus().toString());
          }
        }

        try {
          SegmentUnitTypeThrift segmentUnitType = createSegmentUnitNode.getSegmentUnitType();
          logger.warn("initmembership in createSegment request: {}, seg {}", initMembership, segId);
          boolean imInMemberShip = initMembership.getAliveSecondaries()
              .contains(context.getInstanceId());
          boolean imArbiter = initMembership.getArbiters().contains(context.getInstanceId());
          boolean createAriber = segmentUnitType.equals(SegmentUnitTypeThrift.Arbiter);

          if ((imArbiter && !createAriber) || (imInMemberShip && !imArbiter && createAriber)) {
            throw new Exception("segmentUnitType doesnot match the initmembership");
          }

          segUnit = archiveManager
              .createSegmentUnit(segId, initMembership,
                  createSegmentUnitNode.getSegmentWrapSize(),
                  volumeType, storagePoolId, RequestResponseHelper
                      .convertFromSegmentUnitTypeThrift(
                          createSegmentUnitNode.getSegmentUnitType()), volumeMetadataJson,
                  accountMetadataJson,
                  createSegmentUnitNode.isSecondaryCandidate(),
                  createSegmentUnitNode.isSetReplacee()
                      ? new InstanceId(createSegmentUnitNode.getReplacee()) :
                      null);

          if (volumeSource.getValue() == VolumeSourceThrift.CREATE.getValue()) {
            segUnit.getSegmentUnitMetadata()
                .setVolumeSource(VolumeMetadata.VolumeSourceType.CREATE_VOLUME);
          }

          List<SegmentUnit> segmentUnitList = archiveToSegmentUnits
              .get(segUnit.getArchive().getArchiveId());
          if (null == segmentUnitList) {
            segmentUnitList = new ArrayList<>();
            archiveToSegmentUnits.put(segUnit.getArchive().getArchiveId(), segmentUnitList);
          }
          segmentUnitList.add(segUnit);
          waitPersistSegmentUnits++;
        } catch (InsufficientFreeSpaceException e) {
          logger.warn("caught an exception when creating a segment {}", segId, e);
          throw new NotEnoughSpaceExceptionThrift();
        } catch (Exception e) {
          logger.error("caught an exception when creating a segment {}", segId, e);
          throw new TException("Caught an unknown exception when creating a segment unit", e);
        }
      } catch (NotEnoughSpaceExceptionThrift e) {
        logger.error("caught an exception when creating a segment index {}, volumeId {}",
            createSegmentUnitNode.getSegIndex(), volumeId, e);
        failedSegIndexes.add(new CreateSegmentUnitFailedNode(createSegmentUnitNode.getSegIndex(),
            CreateSegmentUnitFailedCodeThrift.NotEnoughSpaceException));
        currentCreatingSegmentUnit.remove(segId);
      } catch (SegmentExistingExceptionThrift e) {
        logger.error("caught an exception when creating a segment index {}, volumeId {}",
            createSegmentUnitNode.getSegIndex(), volumeId, e);
        failedSegIndexes.add(new CreateSegmentUnitFailedNode(createSegmentUnitNode.getSegIndex(),
            CreateSegmentUnitFailedCodeThrift.SegmentExistingException));
        currentCreatingSegmentUnit.remove(segId);
      } catch (SegmentUnitBeingDeletedExceptionThrift e) {
        logger.error("caught an exception when creating a segment index {}, volumeId {}",
            createSegmentUnitNode.getSegIndex(), volumeId, e);
        failedSegIndexes.add(new CreateSegmentUnitFailedNode(createSegmentUnitNode.getSegIndex(),
            CreateSegmentUnitFailedCodeThrift.SegmentUnitBeingDeletedException));
        currentCreatingSegmentUnit.remove(segId);
      } catch (NoMemberExceptionThrift e) {
        logger.error("caught an exception when creating a segment index {}, volumeId {}",
            createSegmentUnitNode.getSegIndex(), volumeId, e);
        failedSegIndexes.add(new CreateSegmentUnitFailedNode(createSegmentUnitNode.getSegIndex(),
            CreateSegmentUnitFailedCodeThrift.NoMemberException));
        currentCreatingSegmentUnit.remove(segId);
      } catch (SegmentOfflinedExceptionThrift e) {
        logger.error("caught an exception when creating a segment index {}, volumeId {}",
            createSegmentUnitNode.getSegIndex(), volumeId, e);
        failedSegIndexes.add(new CreateSegmentUnitFailedNode(createSegmentUnitNode.getSegIndex(),
            CreateSegmentUnitFailedCodeThrift.SegmentOfflinedException));
        currentCreatingSegmentUnit.remove(segId);
      } catch (InternalErrorThrift e) {
        logger.error("caught an exception when creating a segment index {}, volumeId {}",
            createSegmentUnitNode.getSegIndex(), volumeId, e);
        failedSegIndexes.add(new CreateSegmentUnitFailedNode(createSegmentUnitNode.getSegIndex(),
            CreateSegmentUnitFailedCodeThrift.UnknownException));
        currentCreatingSegmentUnit.remove(segId);
      } catch (Throwable e) {
        logger.error("caught an exception when creating a segment index {}, volumeId {}",
            createSegmentUnitNode.getSegIndex(), volumeId, e);
        failedSegIndexes.add(new CreateSegmentUnitFailedNode(createSegmentUnitNode.getSegIndex(),
            CreateSegmentUnitFailedCodeThrift.UnknownException));
        currentCreatingSegmentUnit.remove(segId);
      }
    }

    CountDownLatch countDownLatch = new CountDownLatch(waitPersistSegmentUnits);
    CompletionHandler<Integer, SegmentUnit> completionHandler =
        new CompletionHandler<Integer, SegmentUnit>() {
          @Override
          public void completed(Integer result, SegmentUnit attachment) {
            succeedSegmentUnits.add(attachment);
            countDownLatch.countDown();
          }

          @Override
          public void failed(Throwable exc, SegmentUnit attachment) {
            logger.error(" persist segment unit matedate failed", exc);
            currentCreatingSegmentUnit.remove(attachment.getSegId());
            failedSegIndexes
                .add(new CreateSegmentUnitFailedNode(attachment.getSegId().getIndex(),
                    CreateSegmentUnitFailedCodeThrift.UnknownException));

            try {
              segmentUnitManager.put(attachment);
              archiveManager.addSegmentUnitToEngines(attachment);
              segmentUnitCanDeletingCheck.deleteSegmentUnitWithOutCheck(attachment.getSegId(),
                  false);
            } catch (Throwable ex) {
              logger.error(" remove segment unit from archive manager failed", ex);
            }

            attachment.setSegmentUnitCreateDone(true);
            countDownLatch.countDown();
          }
        };

    for (Map.Entry entry : archiveToSegmentUnits.entrySet()) {
      List<SegmentUnit> segmentUnitList = (List<SegmentUnit>) entry.getValue();
      for (SegmentUnit segmentUnit : segmentUnitList) {
        SegmentUnitMetadataWriteTask writeTask = new SegmentUnitMetadataWriteTask(
            persistSegmentUnitEngine, segmentUnit, completionHandler);

        persistSegmentUnitEngine.submit(writeTask);
      }
    }

    logger.warn("wait segment units persist matedata {}", archiveToSegmentUnits);
    try {
      countDownLatch.await(cfg.getTimeoutOfCreateSegmentsMs(), TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      logger.error("count down latch catch a interrupt exception", e);
    }

    logger.warn("get segment units persist matedata response ");
    Collections.sort(succeedSegmentUnits, new Comparator<SegmentUnit>() {
      @Override
      public int compare(SegmentUnit o1, SegmentUnit o2) {
        return o1.getSegId().getIndex() - o2.getSegId().getIndex();
      }
    });
    for (SegmentUnit attachment : succeedSegmentUnits) {
      try {
        segmentUnitManager.put(attachment);
        archiveManager.addSegmentUnitToEngines(attachment);
        attachment.setPlalEngine(plalEngine);
        succeedSegIndexes.add(attachment.getSegId().getIndex());
      } catch (Exception e) {
        logger.error("create segment unit failed after persist metadata", e);
        failedSegIndexes.add(new CreateSegmentUnitFailedNode(attachment.getSegId().getIndex(),
            CreateSegmentUnitFailedCodeThrift.UnknownException));
      } finally {
        attachment.setSegmentUnitCreateDone(true);
        currentCreatingSegmentUnit.remove(attachment.getSegId());
      }
    }

    CreateSegmentUnitBatchResponse response = new CreateSegmentUnitBatchResponse(
        request.getRequestId(), volumeId,
        succeedSegIndexes, failedSegIndexes);
    logger.warn("return a create segment unit batch response {}", response);
    return response;
  }

  @Override
  public CreateSegmentUnitResponse createSegmentUnit(CreateSegmentUnitRequest request)
      throws NotEnoughSpaceExceptionThrift, SegmentExistingExceptionThrift,
      SegmentUnitBeingDeletedExceptionThrift, NoMemberExceptionThrift,
      ServiceHavingBeenShutdownThrift,
      InternalErrorThrift, SegmentOfflinedExceptionThrift, TException {
    logger.debug("create segment unit request :{} i am {}", request, context.getInstanceId());

    if (shutdown) {
      throw new ServiceHavingBeenShutdownThrift();
    }
    if (pauseRequestProcessing) {
      throw new ConnectionRefusedExceptionThrift();
    }

    CreateSegmentUnitNode createSegmentUnitNode = new CreateSegmentUnitNode(
        request.getSegIndex(), request.getSegmentUnitType(), request.getSegmentWrapSize(),
        request.getVolumeSource()
    );
    if (request.isSetInitMembership()) {
      createSegmentUnitNode.setInitMembership(request.getInitMembership());
    }
    if (request.isSetInitMembers()) {
      createSegmentUnitNode.setInitMembers(request.getInitMembers());
    }

    if (request.isSetSrcMembership()) {
      createSegmentUnitNode.setSrcMembership(request.getSrcMembership());
    }
    if (request.isSetSecondaryCandidate()) {
      createSegmentUnitNode.setSecondaryCandidate(request.isSecondaryCandidate());
    }
    if (request.isSetReplacee()) {
      createSegmentUnitNode.setReplacee(request.getReplacee());
    }

    List<CreateSegmentUnitNode> unitNodes = new ArrayList<>();
    unitNodes.add(createSegmentUnitNode);
    CreateSegmentUnitBatchRequest createSegmentUnitBatchRequest = new CreateSegmentUnitBatchRequest(
        request.getRequestId(), request.getVolumeId(), request.getVolumeType(),
        request.getStoragePoolId(), request.isEnableLaunchMultiDrivers(),
        unitNodes);
    if (request.isSetVolumeMetadataJson()) {
      createSegmentUnitBatchRequest.setVolumeMetadataJson(request.getVolumeMetadataJson());
    }
    if (request.isSetAccountMetadataJson()) {
      createSegmentUnitBatchRequest.setAccountMetadataJson(request.getAccountMetadataJson());
    }

    CreateSegmentUnitBatchResponse response = this
        .createSegmentUnitBatch(createSegmentUnitBatchRequest);
    if (1 == response.getSuccessedSegsSize()) {
      return new CreateSegmentUnitResponse(request.getRequestId());
    } else if (1 == response.getFailedSegsSize()) {
      switch (response.getFailedSegs().get(0).getErrorCode()) {
        case NotEnoughSpaceException:
          throw new NotEnoughSpaceExceptionThrift();
        case SegmentExistingException:
          throw new SegmentExistingExceptionThrift();
        case NoMemberException:
          throw new NoMemberExceptionThrift();
        case ServiceHavingBeenShutdown:
          throw new ServiceHavingBeenShutdownThrift();
        case SegmentUnitBeingDeletedException:
          throw new SegmentUnitBeingDeletedExceptionThrift();
        case SegmentOfflinedException:
          throw new SegmentOfflinedExceptionThrift();
        default:
          throw new TException();
      }
    } else {
      throw new TException();
    }
  }

  public DeleteSegmentUnitResponse deleteSegmentUnit(DeleteSegmentUnitRequest request)
      throws SegmentNotFoundExceptionThrift, ServiceHavingBeenShutdownThrift,
      StaleMembershipExceptionThrift,
      InternalErrorThrift, TException {
    logger.debug("delete segment unit request :{} ", request);

    if (shutdown) {
      throw new ServiceHavingBeenShutdownThrift();
    }
    if (pauseRequestProcessing) {
      throw new ConnectionRefusedExceptionThrift();
    }

    SegId segId = new SegId(request.getVolumeId(), request.getSegIndex());
    SegmentUnit segUnit = segmentUnitManager.get(segId);
    SegmentMembership requestMembership = DataNodeRequestResponseHelper
        .buildSegmentMembershipFrom(request.getMembership()).getSecond();
    boolean syncPersist = request.isSynPersist();
    if (deleteSegmentUnitInternally(requestMembership, segUnit,
        syncPersist)) {
      return new DeleteSegmentUnitResponse(request.getRequestId());
    } else {
      throw new InternalErrorThrift().setDetail("can't update segment status to Delete");
    }
  }

  @Override
  public DeleteSegmentResponse deleteSegment(DeleteSegmentRequest request)
      throws SegmentNotFoundExceptionThrift, SegmentUnitBeingDeletedExceptionThrift,
      ServiceHavingBeenShutdownThrift, StaleMembershipExceptionThrift,
      InvalidSegmentUnitStatusExceptionThrift,
      InternalErrorThrift, TException {
    logger.debug("delete segment request :{}", request);
    if (shutdown) {
      throw new ServiceHavingBeenShutdownThrift();
    }
    if (pauseRequestProcessing) {
      throw new ConnectionRefusedExceptionThrift();
    }

    SegId segId = new SegId(request.getVolumeId(), request.getSegIndex());
    SegmentUnit segUnit = segmentUnitManager.get(segId);

    SegmentMembershipThrift requestMembershipThrift = request.getMembership();
    SegmentMembership requestMembership = DataNodeRequestResponseHelper
        .buildSegmentMembershipFrom(requestMembershipThrift).getSecond();
    SegmentMembership currentMembership = segUnit.getSegmentUnitMetadata().getMembership();

    InstanceId myself = context.getInstanceId();
    Validator.requestNotHavingStaleMembership(segId, currentMembership, requestMembership);
    Validator.imPrimary(currentMembership, myself.getId());

    BroadcastRequest deleteSegmentUnitRequest = DataNodeRequestResponseHelper
        .buildDeleteSegmentUnitBroadcastRequest(segId, context.getInstanceId().getId(),
            currentMembership);

    try {
      List<EndPoint> endpoints = DataNodeRequestResponseHelper
          .buildEndPoints(instanceStore, currentMembership, true, myself);
      int quorumSize = segUnit.getSegmentUnitMetadata().getVolumeType().getVotingQuorumSize();

      int quorumSizeForResponses = quorumSize - 1;
      Validate.isTrue(quorumSizeForResponses > 0);

      if (endpoints.size() < quorumSizeForResponses) {
        String errString =
            "Can't delete the segment because there are "
                + "no enough members to do so. membership:"
                + currentMembership + " available endpoint: " + endpoints;
        logger.error(errString);
        throw new InternalErrorThrift().setDetail(errString);
      }

      BroadcastResult broadcastResult = dataNodeAsyncClient
          .broadcast(segUnit, cfg.getSecondaryLeaseInPrimaryMs(), deleteSegmentUnitRequest,
              cfg.getDataNodeRequestTimeoutMs(), quorumSizeForResponses, true, endpoints);
      logger.debug("We have received a quorum of logs for DeleteSegmentUnit");
      Map<EndPoint, BroadcastResponse> goodResponses = broadcastResult.getGoodResponses();
      Validate.isTrue(goodResponses.size() >= quorumSizeForResponses);
    } catch (Exception e) {
      String errString = "Can't delete the segment " + segId;
      logger.error(errString, e);
      throw new InternalErrorThrift().setDetail(errString + e.getMessage());
    }

    if (deleteSegmentUnitInternally(requestMembership, segUnit, true)) {
      return new DeleteSegmentResponse();
    } else {
      throw new TException("can't delete segment unit at the primary");
    }
  }

  @Override
  public UpdateSegmentUnitMembershipAndStatusResponse updateSegmentUnitMembershipAndStatus(
      UpdateSegmentUnitMembershipAndStatusRequest request)
      throws InternalErrorThrift, SegmentNotFoundExceptionThrift, InvalidMembershipExceptionThrift,
      StaleMembershipExceptionThrift, ServiceHavingBeenShutdownThrift, TException {
    if (shutdown) {
      throw new ServiceHavingBeenShutdownThrift();
    }
    if (pauseRequestProcessing) {
      throw new ConnectionRefusedExceptionThrift();
    }

    logger.debug("update segment unit's membership and status: {}", request);

    SegId segId = new SegId(request.getVolumeId(), request.getSegIndex());
    SegmentUnit segUnit = segmentUnitManager.get(segId);

    if (segUnit == null
        || segUnit.getSegmentUnitMetadata().getStatus() == SegmentUnitStatus.Deleting
        || segUnit.getSegmentUnitMetadata().getStatus() == SegmentUnitStatus.Deleted) {
      throw new SegmentNotFoundExceptionThrift();
    }

    SegmentMembership currentMembership = segUnit.getSegmentUnitMetadata().getMembership();
    SegmentMembership receivedSegmentMembership = DataNodeRequestResponseHelper
        .buildSegmentMembershipFrom(request.getMembership()).getSecond();
    Validator.requestNotHavingStaleMembership(segId, currentMembership, receivedSegmentMembership);
    Validator
        .requestNotHavingInvalidMembership(segId, currentMembership, receivedSegmentMembership);

    SegmentMembership receivedNewSegmentMembership = null;
    if (request.getNewMembership() != null) {
      receivedNewSegmentMembership = DataNodeRequestResponseHelper
          .buildSegmentMembershipFrom(request.getNewMembership()).getSecond();
    }

    SegmentUnitStatus status = null;
    if (request.getStatus() != null) {
      try {
        status = SegmentUnitStatus.valueOf(request.getStatus());
      } catch (IllegalArgumentException e) {
        throw new InvalidSegmentUnitStatusExceptionThrift();
      }
    }

    try {
      archiveManager.updateSegmentUnitMetadata(segId, receivedNewSegmentMembership, status);
      return new UpdateSegmentUnitMembershipAndStatusResponse(request.getRequestId());
    } catch (Exception e) {
      InternalErrorThrift ie = new InternalErrorThrift();
      ie.setDetail(e.getMessage());
      throw ie;
    }
  }

  @Override
  public UpdateSegmentUnitVolumeMetadataJsonResponse updateSegmentUnitVolumeMetadataJson(
      UpdateSegmentUnitVolumeMetadataJsonRequest request)
      throws InternalErrorThrift, SegmentNotFoundExceptionThrift, InvalidMembershipExceptionThrift,
      StaleMembershipExceptionThrift, NotPrimaryExceptionThrift, ServiceHavingBeenShutdownThrift,
      TException {
    if (shutdown) {
      throw new ServiceHavingBeenShutdownThrift();
    }
    if (pauseRequestProcessing) {
      throw new ConnectionRefusedExceptionThrift();
    }

    logger.debug("update segment unit's volume metadata json: {}", request);
    SegId segId = new SegId(request.getVolumeId(), request.getSegIndex());
    SegmentUnit segUnit = segmentUnitManager.get(segId);

    if (segUnit == null
        || segUnit.getSegmentUnitMetadata().getStatus() == SegmentUnitStatus.Deleting
        || segUnit.getSegmentUnitMetadata().getStatus() == SegmentUnitStatus.Deleted) {
      throw new SegmentNotFoundExceptionThrift();
    }

    SegmentMembership receivedSegmentMembership = DataNodeRequestResponseHelper
        .buildSegmentMembershipFrom(request.getMembership()).getSecond();
    SegmentMembership currentMembership = segUnit.getSegmentUnitMetadata().getMembership();

    Validator.requestNotHavingStaleMembership(segId, currentMembership, receivedSegmentMembership);
    Validator
        .requestNotHavingInvalidMembership(segId, currentMembership, receivedSegmentMembership);

    Validator
        .validatePrimaryStatusAndMembership(segId, segUnit.getSegmentUnitMetadata().getStatus(),
            currentMembership, context);

    logger.info("broadcast volumeMetadata {} to other members in the group",
        request.getVolumeMetadataJson());

    try {
      BroadcastRequest updateSegmentUnitRequest = DataNodeRequestResponseHelper
          .buildUpdateSegmentUnitVolumeMetadataJsonBroadcastRequest(segId, currentMembership,
              request.getVolumeMetadataJson());

      List<EndPoint> endpoints = DataNodeRequestResponseHelper
          .buildEndPoints(instanceStore, currentMembership, true,
              context.getInstanceId());
      int quorumSize = segUnit.getSegmentUnitMetadata().getVolumeType().getVotingQuorumSize();

      int quorumSizeForResponses = quorumSize - 1;
      Validate.isTrue(quorumSizeForResponses > 0);

      if (endpoints.size() < quorumSizeForResponses) {
        String errString =
            "Can't update the volume metadata in segment unit metadata because there are "
                + "no enough members to do so. membership:" + currentMembership
                + " available endpoint: "
                + endpoints;
        logger.error(errString);
        throw new InternalErrorThrift().setDetail(errString);
      }

      BroadcastResult broadcastResult = dataNodeAsyncClient
          .broadcast(segUnit, cfg.getSecondaryLeaseInPrimaryMs(), updateSegmentUnitRequest,
              cfg.getDataNodeRequestTimeoutMs(), quorumSizeForResponses, true, endpoints);
      logger.debug("We have received a quorum of logs for updating volume metadata");
      Map<EndPoint, BroadcastResponse> goodResponses = broadcastResult.getGoodResponses();
      Validate.isTrue(goodResponses.size() >= quorumSizeForResponses);
      logger.info("we have successfully updated volume metadata in secondaries and are "
          + "going to update the primary volume metadata in the unit");

      archiveManager.updateSegmentUnitMetadata(segId, request.getVolumeMetadataJson());
      return new UpdateSegmentUnitVolumeMetadataJsonResponse(request.getRequestId());
    } catch (Exception e) {
      String errString = "Can't update the volume metadata in the segment unit metadata " + segId;
      logger.error(errString, e);
      throw new InternalErrorThrift().setDetail(errString + e.getMessage());
    }
  }

  @Override
  public ArbiterPokePrimaryResponse arbiterPokePrimary(ArbiterPokePrimaryRequest request)
      throws SegmentNotFoundExceptionThrift, StaleMembershipExceptionThrift,
      NotPrimaryExceptionThrift,
      InvalidMembershipExceptionThrift, YouAreNotInMembershipExceptionThrift,
      WrongPrePrimarySessionExceptionThrift, InternalErrorThrift, ServiceHavingBeenShutdownThrift,
      TException {
    if (shutdown) {
      throw new ServiceHavingBeenShutdownThrift();
    }
    if (pauseRequestProcessing) {
      throw new ConnectionRefusedExceptionThrift();
    }

    logger.debug("arbiter poke request {} ", request);

    SegId segId = new SegId(request.getVolumeId(), request.getSegIndex());
    SegmentUnit segUnit = segmentUnitManager.get(segId);

    Validator.validateArbiterPokePrimaryInput(request, segUnit, context);

    ArbiterPokePrimaryResponse response = new ArbiterPokePrimaryResponse(request.getRequestId());

    response.setLatestMembership(DataNodeRequestResponseHelper
        .buildThriftMembershipFrom(segId, segUnit.getSegmentUnitMetadata().getMembership()));
    segUnit
        .extendPeerLease(cfg.getSecondaryLeaseInPrimaryMs(), new InstanceId(request.getMyself()));
    SegmentLogMetadata segLogMetadata = segUnit.getSegmentLogMetadata();

    InstanceId arbiterInstanceId = new InstanceId(request.getMyself());

    long newSwplId = segLogMetadata
        .arbiterUpdatePlId(arbiterInstanceId, cfg.getThresholdToClearSecondaryPlMs());
    segLogMetadata.setSwplIdTo(newSwplId);

    long newSwclId = segLogMetadata
        .arbiterUpdateClId(arbiterInstanceId, cfg.getThresholdToClearSecondaryClMs());
    segLogMetadata.setSwclIdTo(newSwclId);
    logger.debug("going to response: {}", response);

    {
      boolean abnormal = false;
      SegmentMembership membership = RequestResponseHelper
          .buildSegmentMembershipFrom(response.getLatestMembership()).getSecond();

      SegmentMembership requestMembership = RequestResponseHelper
          .buildSegmentMembershipFrom(request.getMembership()).getSecond();

      if (membership.compareEpoch(requestMembership) != 0) {
        logger.warn("why no exception thrown above ? ");
        abnormal = true;
      }

      SegmentUnit segmentUnitAgain = segmentUnitManager.get(segId);
      SegmentMembership localMembership = segmentUnitAgain.getSegmentUnitMetadata()
          .getMembership();

      if (!membership.equals(localMembership)) {
        logger.warn("membership has been changed ?? ");
        abnormal = true;
      }
      if (abnormal) {
        logger.error("what's wrong here ?? {} {} {} {} {}", request, response, segUnit,
            segmentUnitAgain, segUnit == segmentUnitAgain);
      }

    }

    return response;
  }

  private InstanceId pickUpFinalReplacee(SegmentMembership membership, VolumeType volumeType,
      InstanceId replacer,
      InstanceId originalReplacee) {
    Validate.isTrue(membership.isSecondaryCandidate(replacer));
    if (membership.isInactiveSecondary(originalReplacee)) {
      return originalReplacee;
    } else if (!membership.getInactiveSecondaries().isEmpty()) {
      return membership.getInactiveSecondaries().iterator().next();
    } else if (membership.isJoiningSecondary(originalReplacee) || membership
        .isSecondary(originalReplacee)) {
      return originalReplacee;
    } else {
      logger.error("no replacee can be selected {} {} {} {}", volumeType, membership, replacer,
          originalReplacee);
      return null;
    }
  }

  @Override
  public ImReadyToBeSecondaryResponse iAmReadyToBeSecondary(ImReadyToBeSecondaryRequest request)
      throws SegmentNotFoundExceptionThrift, StaleMembershipExceptionThrift,
      NotPrimaryExceptionThrift,
      InvalidMembershipExceptionThrift, YouAreNotInMembershipExceptionThrift,
      WrongPrePrimarySessionExceptionThrift, InternalErrorThrift,
      ServiceHavingBeenShutdownThrift,
      NoOneCanBeReplacedExceptionThrift, org.apache.thrift.TException {
    if (shutdown) {
      throw new ServiceHavingBeenShutdownThrift();
    }
    if (pauseRequestProcessing) {
      throw new ConnectionRefusedExceptionThrift();
    }

    logger.debug("pre-secondary or pre-arbiter is ready to become a secondary {} ", request);

    SegId segId = new SegId(request.getVolumeId(), request.getSegIndex());
    SegmentUnit segUnit = segmentUnitManager.get(segId);

    Validator.validateImReadyToBeSecondaryInput(request, segUnit, context);

    SegmentMembership currentMembership = segUnit.getSegmentUnitMetadata().getMembership();
    InstanceId requestorId = new InstanceId(request.getMyself());
    SegmentMembership newMembership;
    if (request.isSecondaryCandidate() && currentMembership.isSecondaryCandidate(requestorId)) {
      InstanceId finalReplacee = pickUpFinalReplacee(currentMembership,
          segUnit.getSegmentUnitMetadata().getVolumeType(), requestorId,
          new InstanceId(request.getReplacee()));
      if (finalReplacee == null) {
        logger.warn(
            "no one can be replaced by the secondary candidate here, segId: {}, "
                + "current membership: {}, replacer {}, replacee {}",
            segId, currentMembership, request.getMyself(), request.getReplacee());
        throw new NoOneCanBeReplacedExceptionThrift();
      }

      newMembership = currentMembership
          .secondaryCandidateBecomesSecondaryAndRemoveTheReplacee(requestorId, finalReplacee);
      if (newMembership == currentMembership) {
        throw new YouAreNotInMembershipExceptionThrift();
      }
    } else {
      newMembership = currentMembership.joiningSecondaryBecomeSecondary(requestorId);
    }

    try {
      archiveManager.updateSegmentUnitMetadata(segId, newMembership, null);
    } catch (Exception e) {
      logger.warn("caught an exception when updating membership", e);
      throw new InternalErrorThrift().setDetail("can't update membership in primary");
    }

    segUnit.extendPeerLease(cfg.getSecondaryLeaseInPrimaryMs(), requestorId);

    BroadcastRequest joiningGroup = DataNodeRequestResponseHelper
        .buildAddOrRemoveMemberRequest(request.getRequestId(), segId, newMembership,
            segUnit.getSegmentLogMetadata().getSwplId(),
            segUnit.getSegmentLogMetadata().getSwclId());

    try {
      List<EndPoint> endpoints = DataNodeRequestResponseHelper
          .buildEndPoints(instanceStore, newMembership, true,
              context.getInstanceId(), requestorId);

      if (endpoints.size() > 0) {
        dataNodeAsyncClient.broadcast(segUnit, cfg.getSecondaryLeaseInPrimaryMs(), joiningGroup,
            cfg.getDataNodeRequestTimeoutMs(), 0, true, endpoints);
        logger.debug("Successfully broadcast AddOrRemoveMember request to secondaries.");
      }
    } catch (Exception e) {
      logger.info("Caught an exception when broadcasting AddOrRemove to all secendaries. Ignore it",
          e);
    }

    ImReadyToBeSecondaryResponse response = new ImReadyToBeSecondaryResponse(
        request.getRequestId());
    response.setLatestMembership(
        DataNodeRequestResponseHelper.buildThriftMembershipFrom(segId, newMembership));
    logger.debug("going to response: {}", response);
    return response;
  }

  /**
   * Get logs after the given point(exclusive), we will search in the memory first. If not enough
   * logs can be gotten after that, we will search from the storage.
   */
  private PrimaryLogCollection getCommittedLogsAfter(SegId segId,
      SegmentLogMetadata segmentLogMetadata,
      long startLogId, int maxNumberLogs, long requestSecondaryId)
      throws InternalErrorThrift, LogIdTooSmallExceptionThrift {
    Pair<Boolean, List<MutationLogEntry>> searchMemoryResults = segmentLogMetadata
        .getLogsAfterAndCheckCompletion(startLogId, maxNumberLogs, true);

    List<MutationLogEntry> logsAtStorage = new ArrayList<MutationLogEntry>();
    boolean allRequestLogsAtMemory = searchMemoryResults.getFirst();
    if (!allRequestLogsAtMemory) {
      logger.info(
          "parts of the logs after {} that requests for {} might not exist in the memory."
              + " Try log storage system",
          startLogId, requestSecondaryId);
      try {
        logsAtStorage = logStorageReader.readLogsAfter(segId, startLogId,
            maxNumberLogs, true);
      } catch (IOException | KvStoreException e) {
        String errMsg = "Fail to read logs after " + startLogId + " for segId " + segId;
        logger.error(errMsg, e);
        throw new InternalErrorThrift().setDetail(errMsg);
      } catch (LogIdNotFoundException e) {
        LogImage image = segmentLogMetadata.getLogImage(0);
        String errMsg =
            "Logs (segId : " + segId + " after log id: " + startLogId
                + " requested by the secondary "
                + requestSecondaryId + " doesn't exist in the log storage system, my self logImage "
                + image;
        logger.error(errMsg);
        throw new LogIdTooSmallExceptionThrift().setLatestLogId(startLogId);
      }
    }

    List<MutationLogEntry> logsAtMemory = searchMemoryResults.getSecond();
    List<MutationLogEntry> finalLogsAtPrimary = new ArrayList<>();
    int numLogsAtMemory = logsAtMemory.size();
    int numLogsAtStorage = logsAtStorage.size();

    if (numLogsAtMemory > 0 && numLogsAtStorage > 0
        && logsAtStorage.get(numLogsAtStorage - 1).getLogId() < logsAtMemory.get(0).getLogId()) {
      finalLogsAtPrimary.addAll(logsAtStorage);
    } else {
      finalLogsAtPrimary.addAll(logsAtStorage);
      finalLogsAtPrimary.addAll(logsAtMemory);
    }
    Collections.sort(finalLogsAtPrimary);
    return new PrimaryLogCollection(logsAtMemory, logsAtStorage, finalLogsAtPrimary);
  }

  @Override
  public SyncLogsResponse syncLogs(SyncLogsRequest request)
      throws SegmentNotFoundExceptionThrift, LogIdTooSmallExceptionThrift,
      StaleMembershipExceptionThrift,
      InternalErrorThrift, TException,
      NotPrimaryExceptionThrift,
      InvalidMembershipExceptionThrift, ServiceHavingBeenShutdownThrift {
    logger.debug("sync log request {} ", request);
    if (shutdown) {
      throw new ServiceHavingBeenShutdownThrift();
    }
    if (pauseRequestProcessing) {
      throw new ConnectionRefusedExceptionThrift();
    }

    long beginTime = System.currentTimeMillis();
    long allowExecutionTime = cfg.getDataNodeRequestTimeoutMs() * 3 / 4;
    boolean useAllTime = false;

    SegId segId = new SegId(request.getVolumeId(), request.getSegIndex());
    SegmentUnit segUnit = segmentUnitManager.get(segId);

    Validator
        .validateSyncLogInput(request, segUnit, context, request.getRequestSegmentUnitStatus());

    InstanceId requestSender = new InstanceId(request.getMyself());
    SegmentLogMetadata segmentLogMetadata = segUnit.getSegmentLogMetadata();

    PeerStatus peerStatus = PeerStatus.getPeerStatusInMembership(
        segUnit.getSegmentUnitMetadata().getMembership(), requestSender);

    long newSwplId = segmentLogMetadata
        .peerUpdatePlId(request.getPpl(), requestSender, cfg.getThresholdToClearSecondaryPlMs(),
            peerStatus);
    segmentLogMetadata.setSwplIdTo(newSwplId);
    long secondaryCl = request.getPcl();

    long newSwclId = segmentLogMetadata
        .peerUpdateClId(secondaryCl, requestSender, cfg.getThresholdToClearSecondaryClMs(),
            peerStatus);
    final long primaryClId = segmentLogMetadata.getClId();

    segmentLogMetadata.setSwclIdTo(newSwclId);

    List<Long> secondaryUncommittedLogs = request.getLogsAfterCl();
    Map<Long, Long> uuidToLogId = new HashMap<>();
    List<Long> secondaryUuidsOfLogsWithoutLogId = request.getUuidsOfLogsWithoutLogId();
    PrimaryLogCollection primaryLogCollection = getCommittedLogsAfter(
        segId, segmentLogMetadata, secondaryCl,
        Math.max(secondaryUncommittedLogs.size(),
            cfg.getMaxNumberLogsForSecondaryToSyncUp()),
        request.getMyself());
    List<MutationLogEntry> finalLogsAtPrimary = primaryLogCollection.getFinalLogs();

    List<LogThrift> addingLogsToReturn = new ArrayList<LogThrift>(finalLogsAtPrimary.size());

    ExistingLogsForSyncLogResponse existingLogs = new ExistingLogsForSyncLogResponse(
        request.getLogsAfterClSize(), false);
    secondaryUncommittedLogs.add(Long.MAX_VALUE);
    int pageSize = segUnit.getArchive().getArchiveMetadata().getPageSize();
    int totalIoSize = 0;

    int index = 0;
    for (int position = 0; position < secondaryUncommittedLogs.size(); position++) {
      Long logIdAtSecondary = secondaryUncommittedLogs.get(position);

      if (logIdAtSecondary == LogImage.INVALID_LOG_ID) {
        logger.debug("the secondary has given me {} it means it doesn't any logs yet, continue",
            LogImage.INVALID_LOG_ID);
        continue;
      }

      for (; index < finalLogsAtPrimary.size(); ) {
        MutationLogEntry firstLogAtPrimary = finalLogsAtPrimary.get(index);
        if (logIdAtSecondary == firstLogAtPrimary.getLogId()) {
          index++;

          existingLogs.seek(position);
          existingLogs.markLogExisting();
          existingLogs.setLogStatus(position, firstLogAtPrimary.getStatus());
          break;
        } else if (secondaryUuidsOfLogsWithoutLogId.contains(firstLogAtPrimary.getUuid())) {
          uuidToLogId.put(firstLogAtPrimary.getUuid(), firstLogAtPrimary.getLogId());
          index++;
          secondaryUuidsOfLogsWithoutLogId.remove(firstLogAtPrimary.getUuid());
          continue;
        } else if (logIdAtSecondary > firstLogAtPrimary.getLogId()) {
          try {
            index++;
            LogThrift logThrift = null;

            if (request.isSetCatchUpLogId() && firstLogAtPrimary.getLogId() <= request
                .getCatchUpLogId()) {
              logger.info("catch up log id={}, current log={}", request.getCatchUpLogId(),
                  firstLogAtPrimary);

              logThrift = DataNodeRequestResponseHelper
                  .buildLogThriftFrom(segId, firstLogAtPrimary, LogSyncActionThrift.AddMetadata);
            }

            addingLogsToReturn.add(logThrift);
          } catch (Exception e) {
            logger.error(
                "request: {}, logs:{}, logsAtStorage:{},"
                    + " logsAtMemory:{}, logImage: {}, Cause: {}",
                request, firstLogAtPrimary, primaryLogCollection.getLogsAtStorage(),
                primaryLogCollection.getLogsAtMemory(),
                segmentLogMetadata.getLogImage(0), e.getMessage(), e);
            finalLogsAtPrimary.clear();
          }
        } else {
          LogImage logImage = segmentLogMetadata.getLogImage(0);
          logger.info(
              "It seems the primary {} misses a log that a secondary has: {} and the primary's "
                  + "logImage is {} and its primaryFirstLog is {}, logsAtStorage is {} ", segId,
              logIdAtSecondary, logImage, firstLogAtPrimary,
              primaryLogCollection.getLogsAtStorage());
          existingLogs.seek(position);
          existingLogs.markLogMissing();

          break;
        }

        if (addingLogsToReturn.size() >= cfg.getMaxNumberLogsForSecondaryToSyncUp()
            || totalIoSize >= cfg
            .getMaxIoDataSizePerRequestBytes()) {
          break;
        }

        long costTime = System.currentTimeMillis() - beginTime;
        if (costTime >= allowExecutionTime) {
          logger.warn(
              "Syn log has taken {} ms and get {} added logs, "
                  + "we should return the result to secondary:{}",
              costTime, addingLogsToReturn.size(), requestSender);
          useAllTime = true;
          break;
        }
      }

      if (index == finalLogsAtPrimary.size() || addingLogsToReturn.size() >= cfg
          .getMaxNumberLogsForSecondaryToSyncUp() || totalIoSize >= cfg
          .getMaxIoDataSizePerRequestBytes()
          || useAllTime) {
        break;
      }
    }

    if (index != finalLogsAtPrimary.size()) {
      List<MutationLogEntry> subFinalLogsAtPrimary = finalLogsAtPrimary
          .subList(index, finalLogsAtPrimary.size());
      for (long uuid : secondaryUuidsOfLogsWithoutLogId) {
        Long logId = segmentLogMetadata.getMapLogsUuidToLogId().get(uuid);
        if (logId != null && subFinalLogsAtPrimary.stream()
            .anyMatch(log -> log.getLogId() == logId)) {
          uuidToLogId.put(uuid, logId);
        }
      }
    }
    SyncLogsResponse response = new SyncLogsResponse(request.getRequestId(), newSwplId, newSwclId,
        primaryClId,
        uuidToLogId, addingLogsToReturn,
        DataNodeRequestResponseHelper.buildExistingLogsThriftFrom(existingLogs));
    response.setLatestMembership(DataNodeRequestResponseHelper
        .buildThriftMembershipFrom(segId, segUnit.getSegmentUnitMetadata().getMembership()));

    segUnit.extendPeerLease(cfg.getSecondaryLeaseInPrimaryMs(), requestSender);
    return response;
  }

  @Override
  public GetConfigurationsResponse getConfigurations(GetConfigurationsRequest request)
      throws TException {
    logger.debug("Getting configuration : {}", request);
    Map<String, String> allCfgKeyValues = new HashMap<String, String>();

    Method[] methods = DataNodeConfiguration.class.getDeclaredMethods();
    for (Method method : methods) {
      String methodName = method.getName();
      logger.warn("method name:{} in config", methodName);
      char firstCharAtKey;
      String key;
      if (methodName.length() > 2 && methodName.startsWith("is")) {
        firstCharAtKey = methodName.toLowerCase().charAt(2);
        key = methodName.substring(3);
      } else if (methodName.length() > 3 && methodName.startsWith("get")) {
        firstCharAtKey = methodName.toLowerCase().charAt(3);
        key = methodName.substring(4);
      } else {
        continue;
      }

      Object cfgValue;
      try {
        cfgValue = method.invoke(cfg);
        logger.warn("method name:{}, got value:{}", methodName, cfgValue);
      } catch (Exception e) {
        logger.warn("can't invoke:{} ", methodName);
        continue;
      }

      if (cfgValue instanceof Integer || cfgValue instanceof Long || cfgValue instanceof Boolean
          || cfgValue instanceof String || cfgValue instanceof Float
          || cfgValue instanceof Double) {
        allCfgKeyValues.put(firstCharAtKey + key, cfgValue.toString());
      }
    }

    Map<String, String> commonConfigs = DynamicParamConfig.getInstance().getParameter();
    allCfgKeyValues.putAll(commonConfigs);
    GetConfigurationsResponse response;
    Set<String> keys = request.getKeys();
    if (keys == null || keys.size() == 0) {
      response = new GetConfigurationsResponse(request.getRequestId(), allCfgKeyValues);
    } else {
      Map<String, String> results = new HashMap<String, String>(keys.size());
      for (String key : keys) {
        results.put(key, allCfgKeyValues.get(key));
      }
      response = new GetConfigurationsResponse(request.getRequestId(), results);
    }

    return response;
  }

  public void dump() {
    try {
      PageManagerImpl pageManagerImpl = ((PageManagerImpl) pageManager);
      logger.debug("++++++ dump config: ");
      pageManagerImpl.dump();
    } catch (Exception e) {
      logger.info("@@ caught an exception", e);
    }

    try {
      List<SegmentLogMetadata> logMetadatas = mutationLogManager.getSegments(null);
      for (SegmentLogMetadata logMetadata : logMetadatas) {
        logger.debug("now dump segId: {}", logMetadata.getSegId());
        logMetadata.dump();
      }
    } catch (Exception e) {
      logger.warn("@@ caught an exception", e);
    }
  }

  public PbWriteResponse discardData(PyWriteRequest request) throws Exception {
    SettableFuture<Object> future = SettableFuture.create();
    ioService.discard(request.getMetadata(), new AbstractMethodCallback<PbWriteResponse>() {
      @Override
      public void complete(PbWriteResponse object) {
        future.set(object);
      }

      @Override
      public void fail(Exception e) {
        logger.error("can not write", e);
        future.set(e);
      }
    });

    try {
      Object object = future.get();
      if (object instanceof Exception) {
        throw (Exception) object;
      } else {
        return (PbWriteResponse) object;
      }
    } catch (InterruptedException | ExecutionException e1) {
      throw new InternalErrorThrift();
    }
  }

  public PbWriteResponse writeData(PyWriteRequest request) throws Exception {
    SettableFuture<Object> future = SettableFuture.create();
    ioService.write(request, new AbstractMethodCallback<PbWriteResponse>() {
      @Override
      public void complete(PbWriteResponse object) {
        future.set(object);
      }

      @Override
      public void fail(Exception e) {
        logger.error("can not write", e);
        future.set(e);
      }
    });

    try {
      Object object = future.get();
      if (object instanceof Exception) {
        throw (Exception) object;
      } else {
        return (PbWriteResponse) object;
      }
    } catch (InterruptedException | ExecutionException e1) {
      throw new InternalErrorThrift();
    }
  }

  @Override
  public WriteResponse writeData(WriteRequest request)
      throws SegmentNotFoundExceptionThrift, NotPrimaryExceptionThrift, NotSecondaryExceptionThrift,
      ResourceExhaustedExceptionThrift, FailedToBroadcastExceptionThrift,
      ServiceHavingBeenShutdownThrift, InternalErrorThrift, TException {
    return null;
  }

  public PyReadResponse readData(PbReadRequest request) throws Exception {
    SettableFuture<Object> future = SettableFuture.create();
    ioService.read(request, new AbstractMethodCallback<PyReadResponse>() {
      @Override
      public void complete(PyReadResponse object) {
        object.initDataOffsets();
        future.set(object);
      }

      @Override
      public void fail(Exception e) {
        future.set(e);
      }
    });

    try {
      Object object = future.get();
      if (object instanceof Exception) {
        throw (Exception) object;
      } else {
        return (PyReadResponse) object;
      }
    } catch (InterruptedException | ExecutionException e1) {
      throw new InternalErrorThrift();
    }
  }

  @Override
  public ReadResponse readData(ReadRequest request)
      throws SegmentNotFoundExceptionThrift, NotPrimaryExceptionThrift,
      ServiceHavingBeenShutdownThrift, InternalErrorThrift, TException {
    return null;
  }

  @Override
  public SetConfigurationsResponse setConfigurations(SetConfigurationsRequest request)
      throws TException {
    logger.debug("Setting configuration {} ", request);

    Map<String, String> keyValues = request.getConfigurations();
    Map<String, String> results = new HashMap<String, String>(keyValues.size());
    for (Map.Entry<String, String> keyValue : keyValues.entrySet()) {
      String successFlag = "ok";
      String key = keyValue.getKey();
      String value = keyValue.getValue();
      logger.warn("try to set {} to a new value {} ", key, value);

      if (key.equals(DynamicParamConfig.LOG_LEVEL)) {
        DynamicParamConfig.getInstance().setParameter(key, value);
      } else if (key.equals(SWITCH_L2_CACHE)) {
        logger.warn("the function is no longer use");
      } else if (key.equals(ENABLE_LOGGER_TRACER)) {
        String setValue = value.toLowerCase();
        if (setValue.equals("true")) {
          logger.warn("try to enable logger tracer");
          LoggerTracer.getInstance().enableLoggerTracer();
        } else if (setValue.equals("false")) {
          logger.warn("disable logger tracer");
          LoggerTracer.getInstance().disableLoggerTracer();
        } else {
          logger.error("wrong value:{} for config:{}", setValue, ENABLE_LOGGER_TRACER);
        }
      } else if (key.equals(MAX_MIGRATION_SPEED)) {
        logger.warn("do nothing");
      } else {
        String methodName = generateSetterGetterMethodName("set", key);
        String msg = null;
        try {
          Method setter = ReflectionUtils
              .findMethod(DataNodeConfiguration.class, methodName, null,
                  false, false, false);
          Class<?>[] param = setter.getParameterTypes();
          if (param.length != 1) {
            throw new NoSuchMethodException(
                "the number of paramaters for the method " + methodName + " is not 1");
          }
          Class<?> setterParamClass = param[0];
          Object setterParamObject = null;

          if (setterParamClass.isAssignableFrom(String.class)) {
            setterParamObject = (Object) (String.valueOf(value));
          } else if (setterParamClass.isAssignableFrom(Integer.TYPE)) {
            setterParamObject = (Object) (Integer.valueOf(value));
          } else if (setterParamClass.isAssignableFrom(Long.TYPE)) {
            setterParamObject = (Object) (Long.valueOf(value));
          } else if (setterParamClass.isAssignableFrom(Double.TYPE)) {
            setterParamObject = (Object) (Long.valueOf(value));
          } else if (setterParamClass.isAssignableFrom(Boolean.TYPE)) {
            setterParamObject = (Object) (Boolean.valueOf(value));
          } else {
            throw new NoSuchMethodError("no such type " + setterParamClass);
          }
          setter.invoke((Object) cfg, setterParamObject);
        } catch (NoSuchMethodException e) {
          msg = "cant' find the method" + methodName;
          logger.info(msg, e);
          successFlag = msg;
        } catch (Exception e) {
          msg = " Can't invoke the method" + methodName;
          logger.info(msg, e);
          successFlag = msg;
        }
      }
      results.put(key, successFlag);
    }

    SetConfigurationsResponse response = new SetConfigurationsResponse(request.getRequestId(),
        results);
    return response;
  }

  private String generateSetterGetterMethodName(String setterOrGetter, String fieldName) {
    if (fieldName == null || fieldName.length() == 0) {
      return setterOrGetter;
    }

    String upperCaseFieldName = fieldName.toUpperCase();
    char firstChar = upperCaseFieldName.charAt(0);
    if (setterOrGetter == null) {
      setterOrGetter = "";
    }

    return setterOrGetter + firstChar + fieldName.substring(1);
  }

  @Override
  public BroadcastResponse broadcast(BroadcastRequest request)
      throws SegmentNotFoundExceptionThrift, StaleMembershipExceptionThrift,
      ChecksumMismatchedExceptionThrift,
      PrimaryExistsExceptionThrift, LogIdTooSmallExceptionThrift, LeaseExpiredExceptionThrift,
      NotPrimaryExceptionThrift, InvalidParameterValueExceptionThrift, InvalidTokenThrift,
      InvalidSegmentUnitStatusExceptionThrift, InvalidMembershipExceptionThrift,
      ReceiverStaleMembershipExceptionThrift, ServiceHavingBeenShutdownThrift,
      InputHasNoDataExceptionThrift,
      InappropriateLogStatusExceptionThrift, LeaseExtensionFrozenExceptionThrift,
      py.thrift.share.SegmentUnitBeingDeletedExceptionThrift, ResourceExhaustedExceptionThrift,
      InternalErrorThrift, WriteMutationLogExceptionThrift, AbortMutationLogExceptionThrift,
      NotSecondaryExceptionThrift, TException {
    logger.debug("broadcast request :{}", request);
    BroadcastTypeThrift type = request.getLogType();
    if (shutdown && type != BroadcastTypeThrift.BroadcastLogResults) {
      throw new ServiceHavingBeenShutdownThrift();
    }
    if (pauseRequestProcessing) {
      throw new ConnectionRefusedExceptionThrift();
    }

    SegId segId = new SegId(request.getVolumeId(), request.getSegIndex());
    SegmentUnit segUnit = segmentUnitManager.get(segId);

    Validator.validateBroadcastRequest(request, segUnit, context);

    BroadcastResponse response = null;
    switch (type) {
      case VotePrimaryPhase1:
        response = processVotePrimaryPhase1(segUnit, request);
        logger.debug("segId{} complete vote primary phase1", segUnit.getSegId());
        break;
      case VotePrimaryPhase2:
        response = processVotePrimaryPhase2(segUnit, request);
        break;

      case AddOrRemoveMember:
        response = processAddOrRemoveMember(segUnit, request);
        break;
      case WriteMutationLog:
      case BroadcastAbortedLogs:

        response = processWriteMutationLog(segUnit, request);
        break;
      case WriteMutationLogs:
        throw new NotImplementedException();

      case GiveMeYourLogs:
        response = processGiveMeYourLogs(segUnit, request);
        break;
      case CatchUpMyLogs:
        response = processCatchUpMyLogs(segUnit, request);

        if (null != segUnit && null != segUnit.getSegmentLogMetadata()) {
          segUnit.setPrimaryClIdAndCheckNeedPclDriverOrNot(
              segUnit.getSegmentLogMetadata().getPrimaryClId());
        }
        break;
      case JoinMe:
        response = processJoinMe(segUnit, request);
        break;
      case GiveMeYourMembership:
        response = processGiveMeYourMembership(segUnit, request);
        break;
      case DeleteSegmentUnit:
        response = processDeleteSegmentUnit(segUnit, request);
        break;
      case UpdateSegmentUnitVolumeMetadataJson:
        response = processUpdateSegmentUnitVolumeMetadataJson(segUnit, request);
        break;
      case CheckSecondaryIfFullCopyIsRequired:
        response = processCheckSecondaryIfFullCopyIsRequired(segUnit, request);
        break;
      case StepIntoSecondaryEnrolled:
        response = processStepIntoSecondaryEnrolled(segUnit, request);
        break;
      case BroadcastLogResults:
        try {
          response = processtLogResults(segUnit, request);
        } catch (Exception e) {
          logger.error("BroadcastLogResults error {}", e);
          throw e;
        }

        break;
      case GiveYouLogId:
        response = processGiveYouLogId(segUnit, request);
        break;
      case CollectLogsInfo:
        response = processCollectLogInfo(segUnit, request);
        break;
      case PrimaryChanged:
        response = processPrimaryChanged(segUnit, request);
        break;
      case GetPrimaryMaxLogId:
        response = processGetPrimaryMaxLogId(segUnit, request);
        break;
      case StartBecomePrimary:
        response = processStartBecomePrimary(segUnit, request);
        break;
      default:
        logger.error("invalid broadcast type");
        throw new InvalidParameterValueExceptionThrift();
    }

    return response;
  }

  /**
   * data node send all the segment with primary status to heartbeat.
   */
  @Override
  public HeartbeatResponse heartbeat(HeartbeatRequest request)
      throws ServiceHavingBeenShutdownThrift, TException {
    if (shutdown) {
      throw new ServiceHavingBeenShutdownThrift();
    }
    if (pauseRequestProcessing) {
      throw new ConnectionRefusedExceptionThrift();
    }

    List<HeartbeatResponseUnit> heartbeatResponseList = new ArrayList<HeartbeatResponseUnit>();
    for (HeartbeatRequestUnit heartbeatUnit : request.getHeartbeatRequestUnits()) {
      long volumeId = heartbeatUnit.getVolumeId();
      int segIndex = heartbeatUnit.getSegIndex();
      SegId segId = new SegId(volumeId, segIndex);

      SegmentUnit segUnit = segmentUnitManager.get(segId);
      if (segUnit == null) {
        logger.warn("segment unit:{} not found", segId);
        HeartbeatResponseUnit responseUnit = new HeartbeatResponseUnit(volumeId, segIndex,
            HeartbeatResultThrift.SegmentNotFound);
        heartbeatResponseList.add(responseUnit);
        continue;
      }

      HeartbeatResponseUnit responseUnit = processExtendLease(segUnit, heartbeatUnit.getEpoch(),
          heartbeatUnit.isPrePrimary());

      if (responseUnit.getResult() != HeartbeatResultThrift.Success) {
        heartbeatResponseList.add(responseUnit);
      }

      segUnit.setPrimaryClIdAndCheckNeedPclDriverOrNot(heartbeatUnit.getPcl());
    }

    HeartbeatResponse response = new HeartbeatResponse(request.getRequestId(),
        heartbeatResponseList);

    return response;
  }

  /**
   * This class is for test purpose to imitate primary or secondary not do heartbeat.
   */
  @Override
  public HeartbeatDisableResponse heartbeatDisable(HeartbeatDisableRequest request)
      throws SegmentNotFoundExceptionThrift, org.apache.thrift.TException {
    logger.warn("heartbeat disable request {}", request);

    SegId segId = new SegId(request.getVolumeId(), request.getSegIndex());
    SegmentUnit segUnit = segmentUnitManager.get(segId);
    if (segUnit == null) {
      throw new SegmentNotFoundExceptionThrift();
    }

    if (segUnit.getSegmentUnitMetadata().getStatus() == SegmentUnitStatus.Primary) {
      this.hostHeartbeat.removeRelationBetweenPrimaryAndAllAliveSecondaries(segUnit);
      segUnit.setDisableExtendPeerLease(true);
    } else {
      logger.debug("set {} lease hold", segUnit);
      segUnit.setHoldToExtendLease(true);
    }

    return new HeartbeatDisableResponse(request.getRequestId());
  }

  public WriteMutationLogsDisableResponse writeMutationLogDisable(
      WriteMutationLogsDisableRequest request)
      throws SegmentNotFoundExceptionThrift, org.apache.thrift.TException {
    logger.debug("writeMutationLog disable request {}", request);

    SegId segId = new SegId(request.getVolumeId(), request.getSegIndex());
    SegmentUnit segUnit = segmentUnitManager.get(segId);
    if (segUnit == null) {
      throw new SegmentNotFoundExceptionThrift();
    }

    segUnit.setDisableProcessWriteMutationLog(!segUnit.isDisableProcessWriteMutationLog());
    return new WriteMutationLogsDisableResponse(request.getRequestId());
  }

  private BroadcastResponse processWriteMutationLog(SegmentUnit segUnit, BroadcastRequest request)
      throws StaleMembershipExceptionThrift, InvalidMembershipExceptionThrift,
      InvalidSegmentUnitStatusExceptionThrift, InternalErrorThrift,
      ReceiverStaleMembershipExceptionThrift,
      LogIdTooSmallExceptionThrift, InputHasNoDataExceptionThrift, ResourceExhaustedExceptionThrift,
      InappropriateLogStatusExceptionThrift, LeaseExtensionFrozenExceptionThrift {
    SegId segId = segUnit.getSegId();
    SegmentUnitMetadata segmentUnitMetadata = segUnit.getSegmentUnitMetadata();
    SegmentMembership currentMembership = segmentUnitMetadata.getMembership();
    SegmentMembership requestMembership = DataNodeRequestResponseHelper
        .buildSegmentMembershipFrom(request.getMembership()).getSecond();

    Validator.requestNotHavingStaleMembership(segId, currentMembership, requestMembership);
    Validator
        .requestNotHavingStaleMembershipGeneration(segId, currentMembership, requestMembership);

    SegmentUnitStatus status = segmentUnitMetadata.getStatus();
    if (status != SegmentUnitStatus.ModeratorSelected && status != SegmentUnitStatus.Secondary
        && status != SegmentUnitStatus.PreSecondary && status != SegmentUnitStatus.PrePrimary) {
      logger.error("My current status is not ModeratorSelected or Secondary, my status is {}",
          status);
      SegmentUnitStatusThrift statusThrift = status.getSegmentUnitStatusThrift();
      throw new InvalidSegmentUnitStatusExceptionThrift().setMyStatus(statusThrift)
          .setMembership(DataNodeRequestResponseHelper
              .buildThriftMembershipFrom(segId, currentMembership));
    }

    if (status == SegmentUnitStatus.ModeratorSelected && !request.mergeLogs) {
      logger.error("My current status is ModeratorSelected but received a writeData request {} ",
          request);
      SegmentUnitStatusThrift statusThrift = status.getSegmentUnitStatusThrift();
      throw new InvalidSegmentUnitStatusExceptionThrift().setMyStatus(statusThrift)
          .setMembership(
              DataNodeRequestResponseHelper.buildThriftMembershipFrom(segId, currentMembership));
    }

    if (requestMembership.compareEpoch(currentMembership) > 0) {
      Validate.isTrue(false, "Refer to ProcessAddOrRemove for explanation");

      try {
        archiveManager
            .updateSegmentUnitMetadata(segUnit.getSegId(), requestMembership,
                SegmentUnitStatus.Start);
      } catch (Exception e) {
        InternalErrorThrift ie = new InternalErrorThrift();
        ie.setDetail(e.getMessage());
        throw ie;
      }

      throw new ReceiverStaleMembershipExceptionThrift();
    }

    Validate.isTrue(request.getLogType() == BroadcastTypeThrift.WriteMutationLog
        || request.getLogType() == BroadcastTypeThrift.BroadcastAbortedLogs);

    try {
      logger.debug("going to extend my:[{}] lease", segUnit.getSegId());
      segUnit.extendMyLease();
    } catch (LeaseExtensionFrozenException e) {
      throw new LeaseExtensionFrozenExceptionThrift();
    }

    if (segUnit.isArbiter()) {
      return new BroadcastResponse(request.getRequestId(), context.getInstanceId().getId());
    }

    SegmentLogMetadata logMetadata = mutationLogManager.getSegment(segUnit.getSegId());
    if (logMetadata == null) {
      String errMsg = "Can't find log metadata for seg " + segUnit.getSegId();
      logger.error(errMsg);
      throw new InternalErrorThrift().setDetail(errMsg);
    }

    if (request.getLogType() == BroadcastTypeThrift.WriteMutationLog) {
      MutationLogEntry logEntry = logMetadata.getLog(request.getLogId());
      if (Objects.nonNull(logEntry)) {
        LogStatus logStatus = logEntry.getStatus();
        switch (logStatus) {
          case AbortedConfirmed:
            logger.error("a abort confirm log {} will commit", logEntry);
            throw new InternalErrorThrift();
          case Aborted:
            logEntry.setStatus(LogStatus.Created);
            if (request.getStatus() == LogStatusThrift.Committed) {
              segUnit.getSegmentLogMetadata().commitLog(logEntry.getLogId());
            }
            break;
          case Created:
            if (request.getStatus() == LogStatusThrift.Committed) {
              segUnit.getSegmentLogMetadata().commitLog(logEntry.getLogId());
            }
            break;
          default:
        }
      } else {
        if (request.getData() == null) {
          logger.error("{}  has null data ", request);
          throw new InputHasNoDataExceptionThrift();
        }
        try {
          LogStatus logStatus = DataNodeRequestResponseHelper
              .convertFromThriftStatus(request.getStatus());

          MutationLogEntry mutationLog = MutationLogEntryFactory
              .createLogForSyncLog(request.getLogUuid(), request.getLogId(), request.offset,
                  request.getData(), request.getChecksum());
          mutationLog.setStatus(logStatus);
          int pageIndex = (int) (mutationLog.getOffset() / cfg.getPageSize());
          segUnit.setPageHasBeenWrittenAndFirstWritePageIndex(pageIndex);
          logMetadata.insertLogsForSecondary(plalEngine, Lists.newArrayList(mutationLog), 10000);
        } catch (NoAvailableBufferException e) {
          logger.warn("Temporarily out of memory", e);
          throw new ResourceExhaustedExceptionThrift();
        } catch (Exception e) {
          logger.error("caught an unknow error", e);
          throw new InternalErrorThrift().setDetail(e.toString());
        }
      }

    }

    secondaryAddAbortedLogs(logMetadata, request.getAbortedLogs());

    BroadcastResponse response = new BroadcastResponse(request.getRequestId(),
        context.getInstanceId().getId());
    return response;
  }

  /**
   * The PrePrimary member asks for all my logs in order to sync all logs and become the primary.
   */
  private BroadcastResponse processGiveMeYourLogs(SegmentUnit segUnit, BroadcastRequest request)
      throws InvalidTokenThrift, InvalidSegmentUnitStatusExceptionThrift, InternalErrorThrift,
      StaleMembershipExceptionThrift, LeaseExtensionFrozenExceptionThrift {
    SegId segId = segUnit.getSegId();
    RawArchive archive = segUnit.getArchive();

    segUnit.lockStatus();
    boolean persistForce = false;
    try {
      SegmentMembership currentMembership = segUnit.getSegmentUnitMetadata().getMembership();
      SegmentMembership requestMembership = DataNodeRequestResponseHelper
          .buildSegmentMembershipFrom(request.getMembership()).getSecond();

      SegmentUnitStatus status = segUnit.getSegmentUnitMetadata().getStatus();

      if (status != SegmentUnitStatus.ModeratorSelected) {
        InvalidSegmentUnitStatusExceptionThrift e = new InvalidSegmentUnitStatusExceptionThrift();
        e.setMyStatus(status.getSegmentUnitStatusThrift());
        e.setMembership(DataNodeRequestResponseHelper
            .buildThriftMembershipFrom(segId, currentMembership));
        throw e;
      }
      Validator.requestNotHavingStaleEpoch(segId, currentMembership, requestMembership);
      Validator
          .requestNotHavingDifferentTempPrimary(segId, currentMembership, requestMembership);
      if (requestMembership.compareVersion(currentMembership) > 0) {
        logger.warn("we got a higher membership from pre primary {}, current one {}",
            requestMembership, currentMembership);
        try {
          persistForce = archive
              .asyncUpdateSegmentUnitMetadata(segId, requestMembership, null);
        } catch (InvalidSegmentStatusException | StaleMembershipException e) {
          logger.error("can't update membership", e);
          throw new InternalErrorThrift().setDetail(e.toString());
        }
      }

      segUnit.setPotentialPrimaryId(request.getMyself());
    } finally {
      segUnit.unlockStatus();
    }

    if (persistForce) {
      archive.persistSegmentUnitMeta(segUnit.getSegmentUnitMetadata(), true);
    }

    if (segUnit.isArbiter()) {
      BroadcastResponse response = new BroadcastResponse(request.getRequestId(),
          context.getInstanceId().getId());
      response.setLogs(new ArrayList<LogThrift>());
      response.setVolumeMetadataJson(segUnit.getSegmentUnitMetadata().getVolumeMetadataJson());

      response.setPpl(0);
      response.setProgress(ProgressThrift.Done);

      try {
        segUnit.extendMyLease();
      } catch (LeaseExtensionFrozenException e) {
        throw new LeaseExtensionFrozenExceptionThrift();
      }
      return response;
    }

    int maxLogNum = cfg.getGiveMeYourLogsCountEachTime();
    long myInstanceId = this.context.getInstanceId().getId();
    Long primaryReceivedLogId = request.getReceivedLogIdMap().get(myInstanceId);

    List<Long> logIdsPrimaryAlreadyExisted = request.getLogIdsAlreadyHave();
    Set<Long> logIdsPrimaryAlreadyExistedSet = new HashSet<>(logIdsPrimaryAlreadyExisted.size());
    for (Long id : logIdsPrimaryAlreadyExisted) {
      logIdsPrimaryAlreadyExistedSet.add(id);
    }

    Validate.notNull(primaryReceivedLogId);
    SegmentLogMetadata segmentLog = mutationLogManager.getSegment(segUnit.getSegId());
    int tatolIoSize = 0;
    boolean doneWithAllLogs = true;
    try {
      MigrationStatus migrationStatus = segUnit.getSegmentUnitMetadata().getMigrationStatus();
      if (migrationStatus.isMigratedStatus()) {
        logger.warn("it is migration status for segId={}, migration status={}", segId,
            migrationStatus);
        Validate.isTrue(request.getPcl() >= segUnit.getSegmentLogMetadata().getClId()
                || !cfg.isBlockVotingWhenMigratingSecondaryHasMaxPcl(),
            "migrating secondary has a max pcl %d than preprimay %d",
            segUnit.getSegmentLogMetadata().getClId(), request.getPcl());
      }

      List<MutationLogEntry> logsLargerThanPrimaryReceived = segmentLog
          .getLogsAfter(primaryReceivedLogId, maxLogNum);
      List<LogThrift> returnedLogs = new ArrayList<LogThrift>(
          logsLargerThanPrimaryReceived.size());

      long mergeToLogId = request.getLogId();
      for (MutationLogEntry log : logsLargerThanPrimaryReceived) {
        if (log.getLogId() > mergeToLogId) {
          logger.warn("just merge the logs to log id={}, segId={}", mergeToLogId, segId);
          break;
        }

        if (tatolIoSize >= cfg.getMaxIoDataSizePerRequestBytes()) {
          logger.warn(
              "the buffer size of the logs has exceeded "
                  + "the io size of network permit in one request, size: {}",
              tatolIoSize);
          doneWithAllLogs = false;
          break;
        }

        if (logIdsPrimaryAlreadyExistedSet.contains(log.getLogId())) {
          logger.info(
              "logid {}, no matter the data is null or not,"
                  + " but primary has this log, no need load data from page sys",
              log.getLogId());
          returnedLogs.add(DataNodeRequestResponseHelper
              .buildLogThriftFrom(segId, log, LogSyncActionThrift.Add));
          continue;
        }

        byte[] data = log.getData();

        if (log.getStatus() == LogStatus.Committed && data == null) {
          data = fillLogData(segUnit, log, cfg.getPageSize());
          Validate.isTrue(data != null, "log data is null {}", log);
          long calculatedChecksum = 0L;
          long oriChecksum = log.getChecksum();
          if (oriChecksum == 0) {
            log.setChecksum(calculatedChecksum);
          }
          Validate.isTrue(data != null, "log data is null %s", log.toString());
          returnedLogs.add(DataNodeRequestResponseHelper
              .buildLogThriftFrom(segId, log, data, LogSyncActionThrift.Add));
        } else {
          if (log.getStatus() != LogStatus.AbortedConfirmed
              && log.getStatus() != LogStatus.Aborted) {
            Validate.isTrue(data != null, "log data is null %s", log.toString());
            logger.info("the log does not exist in primary, log={}", log);
            long calculatedChecksum = 0L;
            if (calculatedChecksum != log.getChecksum()) {
              logger.info("check log:{} checksum:{}, calc checksum:{}", log.getLogId(),
                  log.getChecksum(),
                  calculatedChecksum);
              log.setChecksum(calculatedChecksum);
            }
          }
          returnedLogs.add(DataNodeRequestResponseHelper
              .buildLogThriftFrom(segId, log, data, LogSyncActionThrift.Add));
        }
        tatolIoSize += log.getLength();
      }

      boolean allLogSentToYou = (returnedLogs.size() < maxLogNum && doneWithAllLogs);
      ProgressThrift progress =
          allLogSentToYou ? ProgressThrift.Done : ProgressThrift.InProgress;
      BroadcastResponse response = new BroadcastResponse(request.getRequestId(),
          context.getInstanceId().getId());
      response.setMaxLogId(segmentLog.getMaxLogId());
      response.setLogs(returnedLogs);
      response.setVolumeMetadataJson(segUnit.getSegmentUnitMetadata().getVolumeMetadataJson());
      response.setPpl(segmentLog.getPlId());
      response.setProgress(progress);

      logger.debug("going to extend my:[{}] lease", segUnit.getSegId());
      segUnit.extendMyLease();

      logger.debug("processGiveMeYourLogs request {}, response {}", request, response);
      return response;
    } catch (Exception e) {
      String errMsg =
          "Caught an unknown exception while getting"
              + " logs during the process of GiveMeYourLogs request at"
              + segId;
      logger.error(errMsg, e);
      throw new InternalErrorThrift().setDetail(errMsg);
    }
  }

  /**
   * This is supposed to be executed in the secondaries. When we got the log ID for a log, will will
   * also insert it to the logs sequence.
   */
  private BroadcastResponse processGiveYouLogId(SegmentUnit segUnit, BroadcastRequest request)
      throws TException {
    throw new InternalErrorThrift().setDetail("give you log id not supported any more");
  }

  private BroadcastResponse processtLogResults(SegmentUnit segUnit, BroadcastRequest request)
      throws TException {
    if (segUnit.isArbiter()) {
      return new BroadcastResponse(request.getRequestId(), context.getInstanceId().getId());
    }
    for (Entry<Long, List<BroadcastLogThrift>> entry : request.getCommitLogsRequest()
        .getMapRequestIdToLogs().entrySet()) {
      for (BroadcastLogThrift logThrift : entry.getValue()) {
        if (segUnit.getSegmentLogMetadata().getClId() > logThrift.getLogId()) {
          continue;
        }
        MutationLogEntry logEntry = segUnit.getSegmentLogMetadata()
            .getLog(logThrift.getLogId());
        if (Objects.isNull(logEntry)) {
          continue;
        }
        LogStatus logStatus = logEntry.getStatus();

        switch (logThrift.getLogStatus()) {
          case AbortConfirmed:
            Validate.isTrue(logStatus != LogStatus.Committed);
            if (logStatus != LogStatus.AbortedConfirmed) {
              logEntry.confirmAbortAndApply();
              MutationLogEntryFactory.releaseLogData(logEntry);
            }
            break;
          case Committed:
            Validate.isTrue(logStatus != LogStatus.AbortedConfirmed);
            if (logStatus != LogStatus.Committed) {
              segUnit.getSegmentLogMetadata().commitLog(logEntry.getLogId());
            }
            break;
          case Created:
          case Abort:
          case Creating:
            throw new NotImplementedException();
          default:
        }
      }

    }
    return new BroadcastResponse(request.getRequestId(), context.getInstanceId().getId());
  }

  private BroadcastResponse processCollectLogInfo(SegmentUnit segUnit, BroadcastRequest request)
      throws TException {
    SegId segId = segUnit.getSegId();
    SegmentMembership currentMembership = segUnit.getSegmentUnitMetadata().getMembership();
    SegmentMembership requestMembership = DataNodeRequestResponseHelper
        .buildSegmentMembershipFrom(request.getMembership()).getSecond();

    Validator.requestNotHavingStaleMembership(segId, currentMembership, requestMembership);
    Validator
        .requestNotHavingStaleMembershipGeneration(segId, currentMembership, requestMembership);

    SegmentUnitStatus status = segUnit.getSegmentUnitMetadata().getStatus();
    if (status == SegmentUnitStatus.PrePrimary) {
      logger.warn("some one want to collect log info but I am already PrePrimary {} {}",
          context.getInstanceId(),
          segId);
      throw new PrimaryExistsExceptionThrift();
    }

    if (status.isFinalStatus()) {
      logger.warn("I am already in final status {} {}, refuse the request", segId, status);
      throw new InvalidSegmentUnitStatusExceptionThrift()
          .setMyStatus(status.getSegmentUnitStatusThrift());
    }

    if (status != SegmentUnitStatus.ModeratorSelected) {
      logger.warn(
          "some one want to collect log info but "
              + "I am not in ModeratorSelected status but {}. {} {}",
          status, context.getInstanceId(), segId);
    }

    BroadcastResponse response = new BroadcastResponse(request.getRequestId(),
        context.getInstanceId().getId());

    long valueOfLogIdGenerator = LogImage.INVALID_LOG_ID;
    long maxLogId = LogImage.INVALID_LOG_ID;
    long pcl = LogImage.INVALID_LOG_ID;
    SegmentLogMetadata logMetadata = mutationLogManager.getSegment(segUnit.getSegId());
    if (logMetadata != null) {
      Pair<Long, Long> valueOfLogIdGeneratorAndMaxLogId = logMetadata
          .getValueOfLogIdGeneratorAndMaxLogId();
      valueOfLogIdGenerator = valueOfLogIdGeneratorAndMaxLogId.getFirst();
      maxLogId = valueOfLogIdGeneratorAndMaxLogId.getSecond();
      pcl = logMetadata.getClId();
    }

    if (segUnit.isMigrating()) {
      logger.warn("some one want to collect log info but I am migrating {}, {}",
          segUnit.getSegmentUnitMetadata(), context.getInstanceId());
    }

    response.setPcl(pcl);
    response.setMaxLogId(maxLogId);
    response.setMyStatus(status.getSegmentUnitStatusThrift());
    response.setMyInstanceId(context.getInstanceId().getId());
    response.setLatestValueOfLogIdGenerator(valueOfLogIdGenerator);
    response.setIsSecondaryZombie(segUnit.isSecondaryZombie());
    response.setMigrating(segUnit.isMigrating());
    response.setArbiter(segUnit.isArbiter());
    if (currentMembership.getTempPrimary() != null) {
      response.setTempPrimary(currentMembership.getTempPrimary().getId());
    }

    return response;
  }

  private BroadcastResponse processGetPrimaryMaxLogId(SegmentUnit segUnit,
      BroadcastRequest request) {
    BroadcastResponse response = new BroadcastResponse(request.getRequestId(),
        context.getInstanceId().getId());
    response.setMaxLogId(segUnit.getSegmentLogMetadata().getMaxLogId());
    response.setPcl(segUnit.getSegmentLogMetadata().getClId());
    logger.info("segId:{} status:{} max log id:{} pcl id:{}", segUnit.getSegId(),
        segUnit.getSegmentUnitMetadata().getStatus(), response.getMaxLogId(),
        response.getPcl());
    return response;
  }

  /**
   * the prePrimary will told all members in membership,and then we can do some thing.
   */
  private BroadcastResponse processStartBecomePrimary(SegmentUnit segUnit,
      BroadcastRequest request) throws StaleMembershipExceptionThrift, InternalErrorThrift {
    SegId segId = segUnit.getSegId();

    SegmentMembership currentMembership = segUnit.getSegmentUnitMetadata().getMembership();
    SegmentMembership requestMembership = DataNodeRequestResponseHelper
        .buildSegmentMembershipFrom(request.getMembership()).getSecond();

    Validator.requestNotHavingStaleEpoch(segId, currentMembership, requestMembership);

    segUnit.lockStatus();
    boolean persistForce = false;
    try {
      if (currentMembership.compareEpoch(requestMembership) < 0) {
        logger.warn("segId{} i {} will update membership {}", segId, currentMembership,
            requestMembership);
        archiveManager.asyncUpdateSegmentUnitMetadata(segId, requestMembership,
            SegmentUnitStatus.Start);

      } else {
        Validate.isTrue(currentMembership.compareEpoch(requestMembership) == 0);
      }

      SegmentUnitStatus segmentUnitStatus = segUnit.getSegmentUnitMetadata().getStatus();
      if ((segmentUnitStatus != SegmentUnitStatus.Start
          && segmentUnitStatus != SegmentUnitStatus.ModeratorSelected)
          || segUnit.myLeaseExpired()) {
        logger
            .warn("i {} will become start because {} tell me he will become primary",
                segId, request.getMyself());
        archiveManager.asyncUpdateSegmentUnitMetadata(segId, null,
            SegmentUnitStatus.Start);

      } else if (segmentUnitStatus == SegmentUnitStatus.ModeratorSelected) {
        boolean needUpdateStatus = false;
        try {
          if (!segUnit.extendMyLease(getOtherStatusesLeaseMsWithJitter())) {
            needUpdateStatus = true;
          }
        } catch (LeaseExtensionFrozenException e) {
          needUpdateStatus = true;
        }
        if (needUpdateStatus) {
          archiveManager
              .asyncUpdateSegmentUnitMetadata(segId, null, SegmentUnitStatus.Start);
        }
      }

      segmentUnitStatus = segUnit.getSegmentUnitMetadata().getStatus();
      if (segmentUnitStatus == SegmentUnitStatus.Start) {
        logger.warn(
            "i {} will become ModeratorSelected because {} tell me he will become primary",
            segId, request.getMyself());
        persistForce = archiveManager
            .asyncUpdateSegmentUnitMetadata(segId, null, SegmentUnitStatus.ModeratorSelected);
        segUnit.startMyLease(getOtherStatusesLeaseMsWithJitter());
      }
    } catch (Exception e) {
      logger.error("can not update segment unit metadata {}", segId, e);
      throw new InternalErrorThrift()
          .setDetail("can not update segment unit metadata");
    } finally {
      segUnit.unlockStatus();
    }

    if (persistForce) {
      segUnit.getArchive().persistSegmentUnitMeta(segUnit.getSegmentUnitMetadata(), true);
    }
    BroadcastResponse response = new BroadcastResponse(request.getRequestId(),
        context.getInstanceId().getId());
    return response;
  }

  private BroadcastResponse processPrimaryChanged(SegmentUnit segUnit, BroadcastRequest request)
      throws StaleMembershipExceptionThrift, InternalErrorThrift {
    SegId segId = segUnit.getSegId();
    SegmentMembership currentMembership = segUnit.getSegmentUnitMetadata().getMembership();
    SegmentMembership requestMembership = DataNodeRequestResponseHelper
        .buildSegmentMembershipFrom(request.getMembership()).getSecond();

    Validator.requestNotHavingStaleMembership(segId, currentMembership, requestMembership);
    if (currentMembership.compareEpoch(requestMembership) < 0) {
      try {
        archiveManager.updateSegmentUnitMetadata(segId, requestMembership, SegmentUnitStatus.Start);
      } catch (Exception e) {
        logger.error("can not update segment unit metadata", e);
        throw new InternalErrorThrift().setDetail("can not update segment unit metadata");
      }

      if (requestMembership.getSegmentVersion().getEpoch() - currentMembership.getSegmentVersion()
          .getEpoch()
          != 1) {
        throw new InternalErrorThrift().setDetail("my membership is too old");
      }
    } else {
      Validate.isTrue(currentMembership.compareEpoch(requestMembership) == 0);
    }
    return new BroadcastResponse(request.getRequestId(), context.getInstanceId().getId());
  }

  private BroadcastResponse processCheckSecondaryIfFullCopyIsRequired(SegmentUnit segUnit,
      BroadcastRequest request)
      throws InvalidTokenThrift, InvalidSegmentUnitStatusExceptionThrift, InternalErrorThrift,
      StaleMembershipExceptionThrift {
    SegId segId = segUnit.getSegId();
    SegmentMembership currentMembership = segUnit.getSegmentUnitMetadata().getMembership();
    SegmentMembership requestMembership = DataNodeRequestResponseHelper
        .buildSegmentMembershipFrom(request.getMembership()).getSecond();

    Validator.requestNotHavingStaleMembership(segId, currentMembership, requestMembership);

    SegmentUnitStatus status = segUnit.getSegmentUnitMetadata().getStatus();

    if (status != SegmentUnitStatus.ModeratorSelected) {
      logger.info(
          "fail to check secondary if full copy is required, segid {} my current status is {}",
          segId, status);
      InvalidSegmentUnitStatusExceptionThrift e = new InvalidSegmentUnitStatusExceptionThrift();
      e.setMyStatus(status.getSegmentUnitStatusThrift());
      e.setMembership(
          DataNodeRequestResponseHelper.buildThriftMembershipFrom(segId, currentMembership));
      throw e;
    }

    boolean isForceFullCopy = segUnit
        .isForceFullCopy(cfg, requestMembership, getContext().getInstanceId());
    BroadcastResponse broadcastResponse = new BroadcastResponse(request.getRequestId(),
        getContext().getInstanceId().getId());
    long pcl = segUnit.isArbiter() ? 0 : segUnit.getSegmentLogMetadata().getClId();
    broadcastResponse.setPcl(pcl);
    broadcastResponse.setForceFullCopy(isForceFullCopy);
    return broadcastResponse;
  }

  private BroadcastResponse processStepIntoSecondaryEnrolled(SegmentUnit segUnit,
      BroadcastRequest request)
      throws InvalidTokenThrift, InvalidSegmentUnitStatusExceptionThrift, InternalErrorThrift,
      StaleMembershipExceptionThrift {
    SegId segId = segUnit.getSegId();
    SegmentMembership currentMembership = segUnit.getSegmentUnitMetadata().getMembership();
    SegmentMembership requestMembership = DataNodeRequestResponseHelper
        .buildSegmentMembershipFrom(request.getMembership()).getSecond();

    Validator.requestNotHavingStaleEpoch(segId, currentMembership, requestMembership);

    SegmentUnitStatus status = segUnit.getSegmentUnitMetadata().getStatus();

    if (status != SegmentUnitStatus.ModeratorSelected) {
      logger.info("fail to step into SE, segid {} my current status is {}", segId, status);
      InvalidSegmentUnitStatusExceptionThrift e = new InvalidSegmentUnitStatusExceptionThrift();
      e.setMyStatus(status.getSegmentUnitStatusThrift());
      e.setMembership(
          DataNodeRequestResponseHelper.buildThriftMembershipFrom(segId, currentMembership));
      throw e;
    }

    try {
      archiveManager
          .updateSegmentUnitMetadata(segUnit.getSegId(), null,
              SegmentUnitStatus.SecondaryEnrolled);
    } catch (Exception e) {
      String errMsg =
          "Caught an unknown exception while updating"
              + " segment unit metadata during the process of GiveMeYourLogs request at"
              + segId;
      logger.error(errMsg, e);
      throw new InternalErrorThrift().setDetail(errMsg);
    }

    segUnit.startMyLease(cfg.getOtherStatusesLeaseMs());

    return new BroadcastResponse(request.getRequestId(), context.getInstanceId().getId());
  }

  private void secondaryAddAbortedLogs(SegmentLogMetadata logMetadata,
      List<AbortedLogThrift> listOfAbortedLogsFromPrimary) throws InternalErrorThrift {
    if (listOfAbortedLogsFromPrimary == null || listOfAbortedLogsFromPrimary.size() == 0) {
      return;
    }
    for (AbortedLogThrift logThrift : listOfAbortedLogsFromPrimary) {
      try {
        MutationLogEntry abortedLog = DataNodeRequestResponseHelper
            .buildAbortedLogFromThrift(logThrift);
        logMetadata.insertLog(abortedLog);
      } catch (InappropriateLogStatusException e) {
        logger.debug(
            "Can't add log {} to the secondary because "
                + "there is a log with the same id existing already",
            logThrift, e);
      } catch (LogIdTooSmall e) {
        logger.error("Can't add log {} to the secondary because its id is too small", logThrift, e);
      }
    }
  }

  /**
   * The function needs to do three things: 1. update to the latest membership 2. update to the new
   * status according to the current segment unit status 3. update swpl.
   */
  private BroadcastResponse processAddOrRemoveMember(SegmentUnit segUnit, BroadcastRequest request)
      throws InvalidSegmentUnitStatusExceptionThrift, InternalErrorThrift,
      ReceiverStaleMembershipExceptionThrift, StaleMembershipExceptionThrift,
      InvalidMembershipExceptionThrift,
      LeaseExtensionFrozenExceptionThrift {
    SegId segId = segUnit.getSegId();
    SegmentMembership currentMembership = segUnit.getSegmentUnitMetadata().getMembership();
    SegmentMembership requestMembership = DataNodeRequestResponseHelper
        .buildSegmentMembershipFrom(request.getMembership()).getSecond();

    Validator.requestNotHavingStaleMembership(segId, currentMembership, requestMembership);

    SegmentUnitStatus status = segUnit.getSegmentUnitMetadata().getStatus();
    if (status == SegmentUnitStatus.Primary) {
      InvalidSegmentUnitStatusExceptionThrift e = new InvalidSegmentUnitStatusExceptionThrift();
      e.setMyStatus(SegmentUnitStatusThrift.Primary);
      e.setMembership(
          DataNodeRequestResponseHelper.buildThriftMembershipFrom(segId, currentMembership));
      throw e;
    }

    if (requestMembership.compareEpoch(currentMembership) > 0) {
      Validate.isTrue(false, "I don't think the execution should be here");

      try {
        archiveManager
            .updateSegmentUnitMetadata(segUnit.getSegId(), requestMembership,
                SegmentUnitStatus.Start);
      } catch (Exception e) {
        InternalErrorThrift ie = new InternalErrorThrift();
        ie.setDetail(e.getMessage());
        throw ie;
      }

      throw new ReceiverStaleMembershipExceptionThrift();
    } else {
      Validate.isTrue(SegmentMembershipHelper
              .okToUpdateToHigherMembership(requestMembership, currentMembership,
                  context.getInstanceId(),
                  segUnit.getSegmentUnitMetadata().getVolumeType().getNumMembers()),
          "The membership should be ok to be updated");

      try {
        SegmentUnitStatus newStatus = null;
        if (!requestMembership.contain(context.getInstanceId()) || requestMembership
            .isInactiveSecondary(context.getInstanceId())) {
          logger
              .warn("{} is inactive or not in the request membership {}, change my status to start",
                  context.getInstanceId(), requestMembership);
          newStatus = SegmentUnitStatus.Start;

        }
        archiveManager.updateSegmentUnitMetadata(segUnit.getSegId(), requestMembership, newStatus);
      } catch (Exception e) {
        logger.error("failed to update the metadata ({}.{}) of the segment unit ({}) ",
            request.getMembership(),
            SegmentUnitStatus.Secondary, segUnit.getSegId(), e);
        throw new InternalErrorThrift();
      }
    }

    if (!segUnit.isArbiter() && request.isSetPswcl() && request.isSetPswpl()) {
      SegmentLogMetadata segmentLogMetadata = mutationLogManager.getSegment(segUnit.getSegId());
      segmentLogMetadata.setSwplIdTo(request.getPswpl());
      segmentLogMetadata.setSwclIdTo(request.getPswcl());
    }

    try {
      logger.debug("going to extend my:[{}] lease", segUnit.getSegId());
      segUnit.extendMyLease();
    } catch (LeaseExtensionFrozenException e) {
      throw new LeaseExtensionFrozenExceptionThrift();
    }

    BroadcastResponse response = new BroadcastResponse(request.getRequestId(),
        context.getInstanceId().getId());
    return response;
  }

  /**
   * The Primary wants to delete segment units from other secondaries.
   */
  private BroadcastResponse processDeleteSegmentUnit(SegmentUnit segUnit, BroadcastRequest request)
      throws SegmentNotFoundExceptionThrift, InternalErrorThrift, TException {
    SegId segId = segUnit.getSegId();
    SegmentMembership currentMembership = segUnit.getSegmentUnitMetadata().getMembership();
    SegmentMembership requestMembership = DataNodeRequestResponseHelper
        .buildSegmentMembershipFrom(request.getMembership()).getSecond();

    SegmentUnitStatus status = segUnit.getSegmentUnitMetadata().getStatus();
    if (status != SegmentUnitStatus.Secondary) {
      InvalidSegmentUnitStatusExceptionThrift e = new InvalidSegmentUnitStatusExceptionThrift();
      e.setMyStatus(status.getSegmentUnitStatusThrift());
      e.setMembership(
          DataNodeRequestResponseHelper.buildThriftMembershipFrom(segId, currentMembership));
      throw e;
    }

    if (deleteSegmentUnitInternally(requestMembership, segUnit, true)) {
      return new BroadcastResponse(request.getRequestId(), context.getInstanceId().getId());
    } else {
      throw new TException("can't update segment status to Delete");
    }
  }

  /**
   * The Primary wants to update segment units for other secondaries. Particularly, the primary
   * wants to update volume metadata.
   */
  private BroadcastResponse processUpdateSegmentUnitVolumeMetadataJson(SegmentUnit segUnit,
      BroadcastRequest request)
      throws SegmentNotFoundExceptionThrift, InternalErrorThrift, TException {
    SegId segId = segUnit.getSegId();

    SegmentMembership currentMembership = segUnit.getSegmentUnitMetadata().getMembership();
    SegmentMembership requestMembership = DataNodeRequestResponseHelper
        .buildSegmentMembershipFrom(request.getMembership()).getSecond();
    Validator.requestNotHavingStaleMembership(segId, currentMembership, requestMembership);

    SegmentUnitStatus status = segUnit.getSegmentUnitMetadata().getStatus();
    if (status != SegmentUnitStatus.Secondary) {
      InvalidSegmentUnitStatusExceptionThrift e = new InvalidSegmentUnitStatusExceptionThrift();
      e.setMyStatus(status.getSegmentUnitStatusThrift());
      e.setMembership(
          DataNodeRequestResponseHelper.buildThriftMembershipFrom(segId, currentMembership));
      throw e;
    }

    try {
      archiveManager.updateSegmentUnitMetadata(segId, request.getVolumeMetadataJson());
      logger.debug("going to extend my:[{}] lease", segUnit.getSegId());
      segUnit.extendMyLease();

      return new BroadcastResponse(request.getRequestId(), context.getInstanceId().getId());
    } catch (LeaseExtensionFrozenException e) {
      throw new LeaseExtensionFrozenExceptionThrift();
    } catch (Exception e) {
      logger.error("can't update segment unit metadata", e);
      InternalErrorThrift ie = new InternalErrorThrift();
      ie.setDetail(e.getMessage());
      throw ie;
    }
  }

  /**
   * The Coordinator asks for memberships from all members.
   */
  private BroadcastResponse processGiveMeYourMembership(SegmentUnit segUnit,
      BroadcastRequest request) {
    BroadcastResponse response = new BroadcastResponse(request.getRequestId(),
        context.getInstanceId().getId());

    SegId segId = segUnit.getSegId();
    SegmentMembership currentMembership = segUnit.getSegmentUnitMetadata().getMembership();
    SegmentMembership requestMembership = DataNodeRequestResponseHelper
        .buildSegmentMembershipFrom(request.getMembership()).getSecond();

    SegmentMembership returnedMembership = currentMembership;
    if (currentMembership.compareVersion(requestMembership) < 0) {
      returnedMembership = requestMembership;
    }

    response.setMembership(
        DataNodeRequestResponseHelper.buildThriftMembershipFrom(segId, returnedMembership));
    response.setMyStatus(
        SegmentUnitStatusThrift.valueOf(segUnit.getSegmentUnitMetadata().getStatus().name()));
    return response;
  }

  private HeartbeatResponseUnit processExtendLease(SegmentUnit segUnit, int primaryEpoch,
      boolean prePrimary) {
    long volumeId = segUnit.getSegId().getVolumeId().getId();
    int segIndex = segUnit.getSegId().getIndex();
    SegmentMembership currentMembership = segUnit.getSegmentUnitMetadata().getMembership();
    SegmentUnitStatus status = segUnit.getSegmentUnitMetadata().getStatus();
    SegmentVersion secondaryVersion = currentMembership.getSegmentVersion();
    if (secondaryVersion.getEpoch() > primaryEpoch) {
      logger.debug(
          "my status is {}. My epoch greater than primary:"
              + " mine is {}, primary {}. SegId: {}. Membership: {} ",
          status, secondaryVersion.getEpoch(), primaryEpoch, segUnit.getSegId(), currentMembership);
      HeartbeatResponseUnit responseUnit = new HeartbeatResponseUnit(volumeId, segIndex,
          HeartbeatResultThrift.StaleMembership);
      responseUnit.setEpochInSecondary(secondaryVersion.getEpoch());
      return responseUnit;
    }

    if (status != SegmentUnitStatus.Secondary && status != SegmentUnitStatus.PreSecondary
        && status != SegmentUnitStatus.Arbiter && status != SegmentUnitStatus.PreArbiter

        && (status != SegmentUnitStatus.ModeratorSelected || !prePrimary)) {
      logger.info("current status is not secondary, but {}, pre primary {}", status, prePrimary);
      HeartbeatResponseUnit responseUnit = new HeartbeatResponseUnit(volumeId, segIndex,
          HeartbeatResultThrift.InvalidSegmentUnitStatus);
      return responseUnit;
    }

    try {
      logger
          .debug("going to extend my:[{}] lease:{}", segUnit.getSegId(), cfg.getSecondaryLeaseMs());
      boolean leaseExpired = false;
      if (status == SegmentUnitStatus.ModeratorSelected) {
        leaseExpired = !segUnit.extendMyLease();
      } else {
        leaseExpired = !segUnit.extendMyLease(cfg.getSecondaryLeaseMs());
      }
      if (leaseExpired) {
        logger.warn("segment unit : {}  lease has expired. We can't extend the lease", segUnit);
        return new HeartbeatResponseUnit(volumeId, segIndex, HeartbeatResultThrift.LeaseExpired);
      }
    } catch (LeaseExtensionFrozenException e) {
      return new HeartbeatResponseUnit(volumeId, segIndex,
          HeartbeatResultThrift.LeaseExtensionFrozen);
    }

    return new HeartbeatResponseUnit(volumeId, segIndex, HeartbeatResultThrift.Success);
  }

  public TProcessor getProcessor() {
    return processor;
  }

  public void pauseRequestProcessing() {
    pauseRequestProcessing = true;
    ioService.pauseRequestProcessing();
  }

  public void reviveRequestProcessing() {
    pauseRequestProcessing = false;
    ioService.reviveRequestProcessing();
  }

  public void pauseCatchupLogDrivers(SegId segId) {
    logger.warn("Pause catch up engines: {}", segId);
    if (catchupLogEngine != null) {
      catchupLogEngine.pause(segId);
    }
  }

  public void pausePclDriver(SegId segId) {
    logger.warn("Pause PCL driver: {}", segId);
    if (catchupLogEngine != null) {
      catchupLogEngine
          .pauseSegmentUnitProcessing(new CatchupLogContextKey(segId, CatchupLogDriverType.PCL));
    }
    SegmentUnit segmentUnit = segmentUnitManager.get(segId);
    if (segmentUnit != null) {
      segmentUnit.setPauseCatchUpLogForSecondary(true);
    }
  }

  public void pausePplDriver(SegId segId) {
    logger.warn("Pause PCL driver: {}", segId);
    if (catchupLogEngine != null) {
      catchupLogEngine
          .pauseSegmentUnitProcessing(new CatchupLogContextKey(segId, CatchupLogDriverType.PPL));
    }
  }

  public void restartCatchupLog(SegId segId) {
    logger.warn("Restart catch up engines: {}, catchupLogEngine: {}", segId, catchupLogEngine);
    if (catchupLogEngine != null) {
      catchupLogEngine.revive(segId);
    }
    SegmentUnit segmentUnit = segmentUnitManager.get(segId);
    if (null != segmentUnit) {
      segmentUnit.setPauseCatchUpLogForSecondary(false);
    }
  }

  private byte[] fillLogData(SegmentUnit segmentUnit, MutationLogEntry firstLogAtPrimary,
      int pageSize)
      throws Exception {
    byte[] data = null;
    if (firstLogAtPrimary.getStatus() == LogStatus.Created
        || firstLogAtPrimary.getStatus() == LogStatus.Committed) {
      if ((data = firstLogAtPrimary.getData()) == null) {
        PageContext<Page> pageContext = null;
        PageAddress addressAndType = segmentUnit.getAddressManager()
            .getPhysicalPageAddress(firstLogAtPrimary.getOffset());
        Optional<PageAddress> pageAddress = Optional.of(addressAndType);
        Validate.isTrue(pageAddress.isPresent());
        Validate.isTrue(!GarbagePageAddress.isGarbagePageAddress(pageAddress.get()));

        try {
          pageContext = pageManager.checkoutForRead(pageAddress.get());
          if (!pageContext.isSuccess()) {
            throw pageContext.getCause();
          }

          int offsetWithinPage = (int) (firstLogAtPrimary.getOffset() % pageSize);
          data = new byte[firstLogAtPrimary.getLength()];

          pageContext.getPage().getData(offsetWithinPage, data, 0,
              firstLogAtPrimary.getLength());
        } catch (ChecksumMismatchedException e) {
          logger.error(
              "Caught a checksum mismatched exception."
                  + " PageAddress={} submitting a request to correct the page",
              pageAddress);
          SegmentMembership membership = segmentUnit.getSegmentUnitMetadata().getMembership();
          PageErrorCorrector corrector = buildPageErrorCorrector(pageAddress.get(), membership,
              segmentUnit);
          corrector.correctPage();
          throw e;
        } catch (Exception e) {
          logger.error("Can't get the page for read. PageAddress: {}, firstLogAtPrimary: {}",
              pageAddress,
              firstLogAtPrimary, e);
          throw e;
        } finally {
          pageContext.updateSegId(segmentUnit.getSegId());
          pageManager.checkin(pageContext);
        }
      } else {
        return data;
      }
    }
    return data;
  }

  private boolean deleteSegmentUnitInternally(SegmentMembership requestMembership,
      SegmentUnit segUnit, boolean syncPersist)
      throws SegmentNotFoundExceptionThrift, ServiceHavingBeenShutdownThrift,
      StaleMembershipExceptionThrift,
      InternalErrorThrift, TException {
    if (segUnit == null) {
      throw new SegmentNotFoundExceptionThrift();
    }

    if (shutdown) {
      throw new ServiceHavingBeenShutdownThrift();
    }
    SegId segId = segUnit.getSegId();

    if (segUnit.getSegmentUnitMetadata().isSegmentUnitDeleted() || segUnit.getSegmentUnitMetadata()
        .isSegmentUnitMarkedAsDeleting()) {
      return true;
    }

    SegmentMembership currentMembership = segUnit.getSegmentUnitMetadata().getMembership();
    Validator.requestNotHavingStaleMembership(segId, currentMembership, requestMembership);

    try {
      segmentUnitCanDeletingCheck.deleteSegmentUnitWithOutCheck(segId, syncPersist);
      return true;
    } catch (Exception e) {
      logger.error("caught an unknown exception", e);
      return false;
    }
  }

  public GenericAsyncClientFactory<AsyncDataNode.AsyncIface> getIoClientFactory() {
    return ioClientFactory;
  }

  public void setIoClientFactory(GenericAsyncClientFactory ioClientFactory) {
    this.ioClientFactory = ioClientFactory;
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

  public SegmentUnitManager getSegMetadataManager() {
    return segmentUnitManager;
  }

  public PageManager<Page> getPageManager() {
    return pageManager;
  }

  public void setPageManager(PageManager<Page> pageManager) {
    this.pageManager = pageManager;
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

  public MutationLogManager getMutationLogManager() {
    return mutationLogManager;
  }

  public void setMutationLogManager(MutationLogManager mutationLogManager) {
    this.mutationLogManager = mutationLogManager;
  }

  public InstanceStore getInstanceStore() {
    return instanceStore;
  }

  public void setInstanceStore(InstanceStore instanceStore) {
    this.instanceStore = instanceStore;
  }

  public LogStorageReader getLogStorageReader() {
    return logStorageReader;
  }

  public void setLogStorageReader(LogStorageReader logStorageReader) {
    this.logStorageReader = logStorageReader;
  }

  public LogPersister getLogPersister() {
    return logPersister;
  }

  public void setLogPersister(LogPersister logPersister) {
    this.logPersister = logPersister;
  }

  public SegmentUnitTaskExecutor getCatchupLogEngine() {
    return catchupLogEngine;
  }

  public void setCatchupLogEngine(SegmentUnitTaskExecutor catchupLogEngine) {
    this.catchupLogEngine = catchupLogEngine;
  }

  public void setCatchupLogEngines(SegmentUnitTaskExecutorImpl catchupLogEngine) {
    this.catchupLogEngine = catchupLogEngine;
  }

  public GenericThriftClientFactory<DataNodeService.AsyncIface> getDataNodeAsyncClientFactory() {
    return dataNodeAsyncClientFactory;
  }

  public void setDataNodeAsyncClientFactory(
      GenericThriftClientFactory<DataNodeService.AsyncIface> dataNodeAsyncClientFactory) {
    this.dataNodeAsyncClientFactory = dataNodeAsyncClientFactory;
    this.dataNodeAsyncClient = new DataNodeServiceAsyncClientWrapper(dataNodeAsyncClientFactory);
  }

  public GenericThriftClientFactory<DataNodeService.Iface> getDataNodeSyncClientFactory() {
    return dataNodeSyncClientFactory;
  }

  public void setDataNodeSyncClientFactory(
      GenericThriftClientFactory<DataNodeService.Iface> dataNodeSyncClientFactory) {
    this.dataNodeSyncClientFactory = dataNodeSyncClientFactory;
  }

  public ThriftAppEngine getDataNodeAppEngine() {
    return appEngine;
  }

  public void setDataNodeAppEngine(ThriftAppEngine dataNodeAppEngine) {
    this.appEngine = dataNodeAppEngine;
  }

  public void setdataNodeAsyncClient(DataNodeServiceAsyncClientWrapper client) {
    this.dataNodeAsyncClient = client;
  }

  public StorageExceptionHandler getStorageExceptionHandler() {
    return storageExceptionHandler;
  }

  public void setStorageExceptionHandler(StorageExceptionHandler storageExceptionHandler) {
    this.storageExceptionHandler = storageExceptionHandler;
  }

  public TaskEngine getCopyPageTaskEngine() {
    return copyPageTaskEngine;
  }

  public void setCopyPageTaskEngine(TaskEngine copyPageTaskEngine) {
    this.copyPageTaskEngine = copyPageTaskEngine;
  }

  private int pageIndex(long pos) {
    return (int) (pos / (long) cfg.getPageSize());
  }

  public boolean belongToSamePage(long offsetInSeg1, long offsetInSeg2) {
    return (pageIndex(offsetInSeg1) == pageIndex(offsetInSeg2));
  }

  public void setStateProcessingEngine(SegmentUnitTaskExecutor stateProcessingEngine) {
    this.stateProcessingEngine = stateProcessingEngine;
  }

  @Override
  public SecondaryCopyPagesResponse secondaryCopyPages(SecondaryCopyPagesRequest request)
      throws SegmentNotFoundExceptionThrift, NotPrimaryExceptionThrift,
      InvalidMembershipExceptionThrift,
      StaleMembershipExceptionThrift, ServiceHavingBeenShutdownThrift, TException,
      InternalErrorThrift, PrimaryNeedRollBackFirstExceptionThrift,
      SecondarySnapshotVersionTooHighExceptionThrift {
    logger.debug("PreSecondary catch up log request: {} ", request);

    if (shutdown) {
      throw new ServiceHavingBeenShutdownThrift();
    }

    if (pauseRequestProcessing) {
      throw new ConnectionRefusedExceptionThrift();
    }

    SegId segId = new SegId(request.getVolumeId(), request.getSegIndex());
    SegmentUnit segUnit = segmentUnitManager.get(segId);

    Validator.validateSecondaryCopyPagesInput(request, segUnit, context);

    SegmentLogMetadata segmentLogMetadata = mutationLogManager.getSegment(segId);
    InstanceId secondaryId = new InstanceId(request.getMyself());

    PeerStatus peerStatus = PeerStatus.getPeerStatusInMembership(
        segUnit.getSegmentUnitMetadata().getMembership(), secondaryId);

    long newSwplId = segmentLogMetadata
        .peerUpdatePlId(request.getPpl(), secondaryId, cfg.getThresholdToClearSecondaryPlMs(),
            peerStatus);
    segmentLogMetadata.setSwplIdTo(newSwplId);
    long newSwclId = segmentLogMetadata
        .peerUpdateClId(request.getPcl(), secondaryId, cfg.getThresholdToClearSecondaryClMs(),
            peerStatus);
    segmentLogMetadata.setSwclIdTo(newSwclId);

    final SecondaryCopyPagesResponse response;
    long clIdFromSecondary = request.getPcl();

    logger.warn(
        "some secondary(clid={}, instanceId={}) sends copy-page request for segId={}, force={}",
        clIdFromSecondary, request.getMyself(), segId, request.isForceFullCopy());
    boolean secondaryClIdExistInPrimary = segUnit
        .secondaryClIdExistInPrimary(secondaryId, clIdFromSecondary, request.getPpl(),
            logStorageReader, request.isForceFullCopy());

    LogImage logImage = segmentLogMetadata.getLogImageIncludeLogsAfterPcl();

    MutationLogEntry catchUpLog = logImage
        .getCl();
    logger.warn("log image={}, catch up log={}, secondaryClIdExistInPrimary={} for segId={}",
        logImage, catchUpLog,
        secondaryClIdExistInPrimary, segId);
    if (catchUpLog == null) {
      catchUpLog = MutationLogEntry.INVALID_MUTATION_LOG;
    }

    response = new SecondaryCopyPagesResponse();
    response.setRequestId(request.getRequestId());
    response.setSecondaryClIdExistInPrimary(secondaryClIdExistInPrimary);
    response.setPswcl(logImage.getSwclId());
    response.setPswpl(logImage.getSwplId());
    response.setMaxLogId(logImage.getMaxLogId());
    response.setCatchUpLogFromPrimary(
        DataNodeRequestResponseHelper
            .buildLogThriftFrom(segId, catchUpLog, LogSyncActionThrift.Add));
    response.setMembership(DataNodeRequestResponseHelper
        .buildThriftMembershipFrom(segId, segUnit.getSegmentUnitMetadata().getMembership()));

    if (!secondaryClIdExistInPrimary) {
      int pageCountInSegmentUnit = (int) (cfg.getSegmentUnitSize() / cfg.getPageSize());
      BitSet bitSet = new BitSet(pageCountInSegmentUnit);
      SegmentUnitMetadata metadata = segUnit.getSegmentUnitMetadata();
      int pageCount = 0;
      if (!cfg.isForceFullCopy()) {
        for (int pageIndex = 0; pageIndex < pageCountInSegmentUnit; pageIndex++) {
          if (!metadata.isPageFree(pageIndex)) {
            bitSet.set(pageIndex, true);
            pageCount++;
          }
        }
      }
      logger.warn("there are {} page not free for segId={}", pageCount, segId);
      response.setSemgentUnitFreePageBitmap(bitSet.toByteArray());
    }

    return response;
  }

  @Override
  public OnlineDiskResponse onlineDisk(OnlineDiskRequest request)
      throws DiskNotFoundExceptionThrift, DiskHasBeenOnlineThrift, ServiceHavingBeenShutdownThrift,
      InternalErrorThrift, TException {
    if (shutdown) {
      throw new ServiceHavingBeenShutdownThrift();
    }
    if (pauseRequestProcessing) {
      throw new ConnectionRefusedExceptionThrift();
    }

    logger.warn("launchDisk disk request is {}", request);
    Archive archive = null;
    switch (request.getOnlineArchive().getType()) {
      case RAW_DISK:
        archive = archiveManager.getRawArchive(request.getOnlineArchive().getArchiveId());
        if (archive == null) {
          logger.error("disk is not found");
          throw new DiskNotFoundExceptionThrift();
        }
        if (!isArchiveCanBeOfflined((RawArchive) archive)) {
          throw new DiskIsBusyThrift();
        }
        break;
      default:
        logger.warn("l2cache is not implement");
        throw new ArchiveTypeNotSupportExceptionThrift();
    }

    try {
      archive.setArchiveStatus(ArchiveStatus.GOOD);
    } catch (ArchiveStatusException e) {
      logger.error("caught an exception: archive is not offline", e);
      throw new DiskHasBeenOnlineThrift();
    } catch (ArchiveNotExistException e) {
      logger.error("caught an exception: archive does not exist", e);
      throw new DiskNotFoundExceptionThrift();
    } catch (Exception e) {
      logger.error("caught an unknown exception", e);
      throw new InternalErrorThrift();
    }

    return new OnlineDiskResponse(request.getRequestId());
  }

  @Override
  public OfflineDiskResponse offlineDisk(OfflineDiskRequest request)
      throws DiskNotFoundExceptionThrift, DiskHasBeenOfflineThrift, ServiceHavingBeenShutdownThrift,
      InternalErrorThrift, DiskIsBusyThrift, ArchiveTypeNotSupportExceptionThrift, TException {
    if (shutdown) {
      throw new ServiceHavingBeenShutdownThrift();
    }
    if (pauseRequestProcessing) {
      throw new ConnectionRefusedExceptionThrift();
    }

    logger.warn("offlineDisk disk request is {}", request);

    Archive archive = null;
    switch (request.getOfflineArchive().getType()) {
      case RAW_DISK:
        archive = archiveManager.getRawArchive(request.getOfflineArchive().getArchiveId());
        if (archive == null) {
          throw new DiskNotFoundExceptionThrift();
        }
        if (!isArchiveCanBeOfflined((RawArchive) archive)) {
          throw new DiskIsBusyThrift();
        }
        break;
      default:
        logger.warn("l2cache is not implement");
        throw new ArchiveTypeNotSupportExceptionThrift();
    }

    try {
      archive.setArchiveStatus(ArchiveStatus.OFFLINING);
    } catch (ArchiveStatusException e) {
      logger.error("caught an exception: archive already offline", e);
      throw new DiskHasBeenOfflineThrift();
    } catch (ArchiveNotExistException e) {
      logger.error("archive is not found ", e);
      throw new DiskNotFoundExceptionThrift();
    } catch (Exception e) {
      logger.error("caught an unknown exception", e);
      throw new InternalErrorThrift();
    }

    return new OfflineDiskResponse(request.getRequestId());
  }

  @Deprecated
  @Override
  public OnlineDiskResponse fixBrokenDisk(OnlineDiskRequest request)
      throws DiskNotFoundExceptionThrift, DiskNotBrokenThrift, ServiceHavingBeenShutdownThrift,
      InternalErrorThrift, ArchiveTypeNotSupportExceptionThrift, org.apache.thrift.TException {
    if (shutdown) {
      throw new ServiceHavingBeenShutdownThrift();
    }
    if (pauseRequestProcessing) {
      throw new ConnectionRefusedExceptionThrift();
    }

    logger.warn("fixBrokenDisk disk request is {}", request);

    Archive archive = null;
    switch (request.getOnlineArchive().getType()) {
      case RAW_DISK:
        archive = archiveManager.getRawArchive(request.getOnlineArchive().getArchiveId());
        if (archive == null) {
          throw new DiskNotFoundExceptionThrift();
        }
        if (!isArchiveCanBeOfflined((RawArchive) archive)) {
          throw new DiskIsBusyThrift();
        }
        break;
      default:
        logger.warn("l2cache is not implement");
        throw new ArchiveTypeNotSupportExceptionThrift();
    }
    try {
      if (archive.getArchiveMetadata().getStatus() != ArchiveStatus.BROKEN) {
        throw new DiskNotBrokenThrift();
      }

      archive.setArchiveStatus(ArchiveStatus.GOOD);
    } catch (Exception e) {
      logger.error("caught an unknown exception", e);
      throw new InternalErrorThrift();
    }

    return new OnlineDiskResponse(request.getRequestId());
  }

  @Override
  public OnlineDiskResponse fixConfigMismatchedDisk(OnlineDiskRequest request)
      throws DiskNotFoundExceptionThrift, DiskNotMismatchConfigThrift,
      ServiceHavingBeenShutdownThrift,
      InternalErrorThrift, ArchiveTypeNotSupportExceptionThrift, TException {
    if (shutdown) {
      throw new ServiceHavingBeenShutdownThrift();
    }
    if (pauseRequestProcessing) {
      throw new ConnectionRefusedExceptionThrift();
    }

    logger.warn("fixConfigMismatchedDisk disk request is {}", request);
    Archive archive = null;
    switch (request.getOnlineArchive().getType()) {
      case RAW_DISK:
        archive = archiveManager.getRawArchive(request.getOnlineArchive().getArchiveId());
        if (archive == null) {
          throw new DiskNotFoundExceptionThrift();
        }
        if (!isArchiveCanBeOfflined((RawArchive) archive)) {
          throw new DiskIsBusyThrift();
        }
        break;
      default:
        logger.warn("l2cache is not implement");
        throw new ArchiveTypeNotSupportExceptionThrift();
    }

    try {
      if (archive.getArchiveMetadata().getStatus() != ArchiveStatus.CONFIG_MISMATCH) {
        throw new DiskNotMismatchConfigThrift();
      }
      archive.setArchiveStatus(ArchiveStatus.GOOD);
    } catch (Exception e) {
      logger.error("caught an unknown exception", e);
      throw new InternalErrorThrift();
    }

    return new OnlineDiskResponse(request.getRequestId());
  }

  @Override
  public SetIoErrorResponse setIoError(SetIoErrorRequest request)
      throws DiskNotFoundExceptionThrift, ServiceHavingBeenShutdownThrift, InternalErrorThrift,
      org.apache.thrift.TException {
    logger.warn("setIOError request is {}", request);
    if (shutdown) {
      throw new ServiceHavingBeenShutdownThrift();
    }
    if (pauseRequestProcessing) {
      throw new ConnectionRefusedExceptionThrift();
    }

    Archive archiveBase = null;
    long archiveId = request.getArchiveId();
    if (request.getArchiveType() == null
        || request.getArchiveType() == ArchiveTypeThrift.RAW_DISK) {
      archiveBase = archiveManager.getRawArchive(archiveId);
      if (null == archiveBase) {
        throw new DiskNotFoundExceptionThrift();
      }
    } else {
      throw new DiskNotFoundExceptionThrift();
    }

    for (int i = 0; i < request.getErrorCount(); i++) {
      storageExceptionHandler.handle(archiveBase.getStorage(),
          new StorageException("Simulating an I/O exception").setIoException(true));
    }

    return new SetIoErrorResponse(request.getRequestId());
  }

  public SetArchiveConfigResponse setArchiveConfig(SetArchiveConfigRequest request)
      throws DiskNotFoundExceptionThrift, ServiceHavingBeenShutdownThrift, InternalErrorThrift,
      org.apache.thrift.TException {
    if (shutdown) {
      throw new ServiceHavingBeenShutdownThrift();
    }
    if (pauseRequestProcessing) {
      throw new ConnectionRefusedExceptionThrift();
    }

    long archiveId = request.getArchiveId();
    Archive archive = archiveManager.getRawArchive(archiveId);
    if (null == archive) {
      throw new DiskNotFoundExceptionThrift();
    }

    try {
      archiveManager.setArchiveConfig(archiveId, request);
    } catch (JsonProcessingException | StorageException e) {
      logger.warn("caught an exception", e);
      throw new InternalErrorThrift().setDetail("fail to set the archive=" + archiveId);
    }

    return new SetArchiveConfigResponse(request.getRequestId());
  }

  private boolean isArchiveCanBeOfflined(RawArchive archive) {
    Collection<SegmentUnit> segmentUnits = archive.getSegmentUnits();
    InstanceId myInstanceId = this.context.getInstanceId();

    for (SegmentUnit unit : segmentUnits) {
      SegmentUnitMetadata metadata = unit.getSegmentUnitMetadata();
      SegmentUnitStatus myStatus = metadata.getStatus();

      if (myStatus.isFinalStatus()) {
        continue;
      }

      if (myStatus != SegmentUnitStatus.Primary && myStatus != SegmentUnitStatus.Secondary
          && myStatus != SegmentUnitStatus.Arbiter) {
        logger
            .warn("myself can not be offlined, segId {}, status {}", metadata.getSegId(), myStatus);
        return false;
      }

      SegmentMembership membership = metadata.getMembership();
      if (membership.getAliveSecondaries().size() + 1 < metadata.getVolumeType().getNumMembers()) {
        logger.warn("membership is lack of member, segment {}", metadata);
        return false;
      }

      Set<InstanceId> peerIdList = membership
          .getPeerInstanceIds(new InstanceId(myInstanceId.getId()));

      BroadcastRequest request = RequestResponseHelper
          .buildGiveMeYourMembershipRequest(RequestIdBuilder.get(), unit.getSegId(), membership,
              myInstanceId.getId());
      for (InstanceId peerId : peerIdList) {
        Instance peerInstance = this.instanceStore.get(peerId);
        DataNodeService.Iface dataNodeClient = null;
        BroadcastResponse response;
        try {
          dataNodeClient = dataNodeSyncClientFactory.generateSyncClient(peerInstance.getEndPoint());
          response = dataNodeClient.broadcast(request);
          SegmentUnitStatus peerStatus = SegmentUnitStatus.valueOf(response.getMyStatus().name());

          if (peerStatus != SegmentUnitStatus.Primary && peerStatus != SegmentUnitStatus.Secondary
              && peerStatus != SegmentUnitStatus.Arbiter) {
            logger.warn(
                "peer is not primary or secondary or arbiter, segId {}, peerId {}, peer status {}",
                unit.getSegId(), peerId, peerStatus);
            return false;
          }

          SegmentMembership peerMembership = RequestResponseHelper
              .buildSegmentMembershipFrom(response.getMembership()).getSecond();
          if (!membership.equals(peerMembership)) {
            logger.warn("peer membership is not equals to me. my membership {}, peer membership {}",
                membership, peerMembership);
            return false;
          }
        } catch (GenericThriftClientFactoryException | TException e1) {
          logger.error("ask the membership catch an exception ", e1);
          return false;
        } finally {
          dataNodeSyncClientFactory.releaseSyncClient(dataNodeClient);
        }
      }
    }

    return true;
  }

  @Override
  public InvalidateCacheResponse invalidateCache(InvalidateCacheRequest request)
      throws org.apache.thrift.TException, NotSupportedExceptionThrift {
    logger.info("a client is invalidating all pages in "
        + "the page system. This should be only used for the testing purpose");
    for (SegmentUnit segmentUnit : segmentUnitManager.get()) {
      SegmentLogMetadata logMetadata = segmentUnit.getSegmentLogMetadata();
      if (logMetadata.getNumLogsAfterPcl() != 0 || logMetadata.getNumLogsFromPlalToPcl() != 0) {
        logger.warn("still have logs in log system {}", logMetadata);
        return new InvalidateCacheResponse(false);
      }
    }
    if (pageManager instanceof PageManagerImpl) {
      boolean done = false;
      if (request.getCacheLevel() == 1) {
        done = pageManager.cleanCache();
        if (!done) {
          logger.warn("still have dirty pages in page system", pageManager.getDirtyPageCount());
        }
        return new InvalidateCacheResponse(done);
      } else if (request.getCacheLevel() == 2) {
        return new InvalidateCacheResponse(done);
      } else {
        throw new NotSupportedExceptionThrift().setDetail("unknown cache level");
      }
    } else {
      throw new NotSupportedExceptionThrift()
          .setDetail("page system doesn't support this function");
    }
  }

  public PlalEngine getPlalEngine() {
    return plalEngine;
  }

  public void setPlalEngine(PlalEngine plalEngine) {
    this.plalEngine = plalEngine;
  }

  @Override
  public GetLatestLogsResponseThrift getLatestLogsFromPrimary(GetLatestLogsRequestThrift request)
      throws InternalErrorThrift, SegmentNotFoundExceptionThrift, NotPrimaryExceptionThrift,
      TException {
    logger.debug("get latest log request: {}", request);
    GetLatestLogsResponseThrift response = new GetLatestLogsResponseThrift();
    response.setRequestId(request.getRequestId());
    List<SegIdThrift> segIndexList = request.getSegIdList();
    Map<SegIdThrift, List<BroadcastLogThrift>> mapSegIdToLogs =
        new HashMap<SegIdThrift, List<BroadcastLogThrift>>();
    response.setMapSegIdToLogs(mapSegIdToLogs);

    SegmentMembership membership = null;
    for (SegIdThrift segIdThrift : segIndexList) {
      SegId segId = RequestResponseHelper.buildSegIdFrom(segIdThrift);
      SegmentUnit segmentUnit = segmentUnitManager.get(segId);
      membership = segmentUnit.getSegmentUnitMetadata().getMembership();

      Validator.validateSegmentUnitExistsAndImPrimary(segmentUnit, context);

      SegmentLogMetadata segmentLogMetadata = segmentUnit.getSegmentLogMetadata();
      List<MutationLogEntry> logs = segmentLogMetadata.getLogsAfterPcl();
      if (logs.isEmpty()) {
        logger
            .error("segIdThrift: {} has not logs, {}", segIdThrift, segmentLogMetadata.getClId());
      }

      List<BroadcastLogThrift> thriftLogs = new ArrayList<BroadcastLogThrift>();
      List<Long> logIds = new ArrayList<Long>();
      for (MutationLogEntry log : logs) {
        thriftLogs.add(MutationLogEntry.buildBroadcastLogFrom(log));
        logIds.add(log.getLogId());
      }

      mapSegIdToLogs.put(segIdThrift, thriftLogs);

      try {
        if (request.isNeedRemoveLogsInSecondary()) {
          List<EndPoint> secondaryEndpoints = DataNodeRequestResponseHelper
              .buildEndPoints(instanceStore, membership, true,
                  context.getInstanceId());
          for (EndPoint secondaryEndPoint : secondaryEndpoints) {
            DataNodeService.Iface client = null;
            try {
              client = dataNodeSyncClientFactory.generateSyncClient(secondaryEndPoint);
              RemoveUncommittedLogsExceptForThoseGivenRequest removeRequest =
                  new RemoveUncommittedLogsExceptForThoseGivenRequest();
              removeRequest.setGivenLogIds(logIds);
              removeRequest.setSegId(segIdThrift);
              removeRequest.setRequestId(RequestIdBuilder.get());
              client.removeUncommittedLogsExceptForThoseGiven(removeRequest);
            } finally {
              dataNodeSyncClientFactory.releaseSyncClient(client);
            }
          }
        }
      } catch (Exception e) {
        logger.warn("can't tell secondaries to remove uncommitted logs", e);
      }

    }

    return response;
  }

  @Override
  public CommitLogsResponseThrift commitLog(CommitLogsRequestThrift request)
      throws InternalErrorThrift, SegmentNotFoundExceptionThrift, InvalidMembershipExceptionThrift,
      StaleMembershipExceptionThrift, NotPrimaryExceptionThrift, NotSecondaryExceptionThrift,
      org.apache.thrift.TException {
    if (shutdown) {
      throw new ServiceHavingBeenShutdownThrift();
    }
    if (pauseRequestProcessing) {
      throw new ConnectionRefusedExceptionThrift();
    }
    Pair<SegId, SegmentMembership> pair = DataNodeRequestResponseHelper
        .buildSegmentMembershipFrom(request.getMembership());
    SegmentMembership requestMembership = pair.getSecond();
    SegId segId = pair.getFirst();
    logger.debug("process commit logs request : {}, segId: {}", request, segId);

    boolean isPrimary = false;
    InstanceId currentInstanceId = context.getInstanceId();
    if (requestMembership.getPrimary().equals(currentInstanceId)) {
      isPrimary = true;
    } else {
      if (requestMembership.getSecondaries().contains(currentInstanceId) || requestMembership
          .getJoiningSecondaries().contains(currentInstanceId)) {
        isPrimary = false;
      } else {
        throw new InvalidMembershipExceptionThrift();
      }
    }

    SegmentUnit segUnit = segmentUnitManager.get(segId);
    if (segUnit == null) {
      throw new SegmentNotFoundExceptionThrift();
    }
    segId = segUnit.getSegId();
    SegmentMembership currentMembership = segUnit.getSegmentUnitMetadata().getMembership();

    if (isPrimary) {
      Validator.requestNotHavingStaleMembershipEpoch(segId, currentMembership, requestMembership);
      Validator.validateSegmentUnitExistsAndImPrimary(segUnit, context);
    } else {
      Validator.requestNotHavingStaleMembership(segId, currentMembership, requestMembership);
      Validator
          .validateSegmentUnitExistsAndImCanWriteSecondaryOrJoiningSecondary(segUnit, context);
      Validator
          .requestNotHavingStaleMembershipGeneration(segId, currentMembership, requestMembership);
    }

    CommitLogsResponseThrift commitLogsResponse = new CommitLogsResponseThrift();

    commitLogsResponse.setEndPoint(context.getMainEndPoint().toString());
    SegmentLogMetadata segLogMetadata = segUnit.getSegmentLogMetadata();

    for (Entry<Long, List<BroadcastLogThrift>> entry : request.getMapRequestIdToLogs()
        .entrySet()) {
      try {
        if (!isPrimary) {
          Map hashMap = new HashMap();
          for (BroadcastLogThrift logThrift : entry.getValue()) {
            Validate.isTrue(logThrift.getLogId() != LogImage.INVALID_LOG_ID);
            hashMap.put(logThrift.getLogUuid(), logThrift.getLogId());
          }
          segLogMetadata.giveYouLogId(hashMap, plalEngine, false, false);
        }

        List<Pair<MutationLogEntry, BroadcastLogThrift>> committedLogs = segLogMetadata
            .commitThriftLogs(entry.getValue(), isPrimary);
        for (Pair<MutationLogEntry, BroadcastLogThrift> element : committedLogs) {
          if (element.getFirst() != null) {
            plalEngine.putLog(segId, element.getFirst());
          }
        }

        logger
            .info("segId: {}, io context manager id: {}, commit logs:{} ", segId, entry.getKey(),
                entry.getValue());
      } catch (Exception e) {
        logger.warn("can not commit log, requestId: {}, isPrimary: {}", entry.getKey(), isPrimary,
            e);
      }
    }

    commitLogsResponse.setMapRequestIdToLogs(request.getMapRequestIdToLogs());
    commitLogsResponse.setRequestId(request.getRequestId());
    logger.debug("processed log results :{}", commitLogsResponse);

    return commitLogsResponse;
  }

  @Override
  public ConfirmAbortLogsResponseThrift confirmAbortLog(ConfirmAbortLogsRequestThrift request)
      throws InternalErrorThrift, SegmentNotFoundExceptionThrift, NotPrimaryExceptionThrift,
      TException {
    if (shutdown) {
      throw new ServiceHavingBeenShutdownThrift();
    }
    if (pauseRequestProcessing) {
      throw new ConnectionRefusedExceptionThrift();
    }
    ConfirmAbortLogsResponseThrift response = new ConfirmAbortLogsResponseThrift();
    response.setRequestId(request.getRequestId());

    SegIdThrift segIdThrift = request.getSegId();
    Validate.notNull(segIdThrift);
    SegId segId = new SegId(segIdThrift.getVolumeId(), segIdThrift.getSegmentIndex());

    SegmentUnit segUnit = segmentUnitManager.get(segId);

    Validator.validateSegmentUnitExistsAndImPrimary(segUnit, context);

    long fromLogId = request.getFromLogId();
    long toLogId = request.getToLogId();

    SegmentLogMetadata segmentLogMetadata = segUnit.getSegmentLogMetadata();
    List<MutationLogEntry> logsToConfirmAbort = segmentLogMetadata
        .getLogIntervals(fromLogId, toLogId);
    long newFromLogId = segmentLogMetadata.confirmAbortLogs(logsToConfirmAbort);

    LogImage logImage = segmentLogMetadata.getLogImageIncludeLogsAfterPcl();
    long clId = logImage.getClId();
    if (clId < toLogId) {
      response.setFromLogId(newFromLogId == 0 ? fromLogId : newFromLogId);
      response.setSuccess(false);
    } else {
      response.setSuccess(true);
    }
    return response;
  }

  @Override
  public RemoveUncommittedLogsExceptForThoseGivenResponse removeUncommittedLogsExceptForThoseGiven(
      RemoveUncommittedLogsExceptForThoseGivenRequest request)
      throws SegmentNotFoundExceptionThrift, NotPrimaryExceptionThrift, TException {
    SegId segId = RequestResponseHelper.buildSegIdFrom(request.getSegId());
    SegmentUnit segmentUnit = segmentUnitManager.get(segId);
    Validator
        .validateSegmentUnitExistsAndImCanWriteSecondaryOrJoiningSecondary(segmentUnit, context);
    SegmentLogMetadata segmentLogMetadata = segmentUnit.getSegmentLogMetadata();

    long maxLogId = request.getGivenLogIds().get(request.getGivenLogIds().size() - 1);
    List<MutationLogEntry> exceedLogs = segmentLogMetadata
        .getLogsAfter(maxLogId, Integer.MAX_VALUE);
    List<Long> uncommitedLogIds = new ArrayList<>();
    logger.info("the logs={} should be removed", exceedLogs);
    for (MutationLogEntry log : exceedLogs) {
      try {
        segmentLogMetadata.removeLog(log.getLogId());
        uncommitedLogIds.add(log.getLogId());
      } catch (Exception e) {
        logger.error("cannot remove the logId {} exception catch", log, e);
      }
    }

    RemoveUncommittedLogsExceptForThoseGivenResponse response =
        new RemoveUncommittedLogsExceptForThoseGivenResponse();
    response.setDeletedLogIds(uncommitedLogIds);
    response.setSegId(request.getSegId());
    response.setRequestId(request.getRequestId());
    return response;
  }

  public HostHeartbeat getHostHeartbeat() {
    return hostHeartbeat;
  }

  public void setHostHeartbeat(HostHeartbeat hostHeartbeat) {
    this.hostHeartbeat = hostHeartbeat;
  }

  public TempLogPersister getTempLogPersister() {
    return tempLogPersister;
  }

  public void setTempLogPersister(TempLogPersister tempLogPersister) {
    this.tempLogPersister = tempLogPersister;
  }

  public boolean isShutdown() {
    return shutdown;
  }

  @Override
  public SetDatanodeDomainResponseThrift setDatanodeDomain(SetDatanodeDomainRequestThrift request)
      throws ServiceHavingBeenShutdownThrift, InvalidInputExceptionThrift,
      DatanodeIsUsingExceptionThrift,
      TException {
    logger.debug(request.toString());
    if (shutdown) {
      logger.error("datanode:{} had been shutdown", context.getMainEndPoint());
      throw new ServiceHavingBeenShutdownThrift();
    }
    if (pauseRequestProcessing) {
      throw new ConnectionRefusedExceptionThrift();
    }
    if (!request.isSetDomainId()) {
      logger.error("Invalid request:{}", request);
      throw new InvalidInputExceptionThrift();
    }
    final SetDatanodeDomainResponseThrift response = new SetDatanodeDomainResponseThrift();

    InstanceDomain instanceDomain = context.getInstanceDomain();
    if (instanceDomain != null && instanceDomain.getDomainId() != null) {
      if (instanceDomain.getDomainId().longValue() != request.getDomainId()) {
        logger.error("datanode is using domain id:{}, but try update with new domain id:{}",
            instanceDomain.getDomainId(), request.getDomainId());
        throw new DatanodeIsUsingExceptionThrift();
      }
    } else {
      instanceDomain = new InstanceDomain();
    }
    instanceDomain.setDomainId(request.getDomainId());
    context.setInstanceDomain(instanceDomain);
    response.setRequestId(request.getRequestId());
    return response;
  }

  @Override
  public FreeDatanodeDomainResponseThrift freeDatanodeDomain(
      FreeDatanodeDomainRequestThrift request)
      throws ServiceHavingBeenShutdownThrift, InvalidInputExceptionThrift, TException {
    logger.debug(request.toString());
    if (shutdown) {
      logger.error("datanode:{} had been shutdown", context.getMainEndPoint());
      throw new ServiceHavingBeenShutdownThrift();
    }
    if (pauseRequestProcessing) {
      throw new ConnectionRefusedExceptionThrift();
    }
    FreeDatanodeDomainResponseThrift response = new FreeDatanodeDomainResponseThrift();
    response.setRequestId(request.getRequestId());

    InstanceDomain instanceDomain = context.getInstanceDomain();
    if (instanceDomain == null) {
      return response;
    }

    Collection<RawArchive> archives = archiveManager.getRawArchives();
    AtomicInteger archiveSize = new AtomicInteger(archives.size());
    for (RawArchive archive : archives) {
      if (archive.getStoragePoolId() != null) {
        try {
          archiveManager.freeArchiveWithOutCheck(archive);
        } catch (AllSegmentUnitDeadInArchviveException aud) {
          try {
            archiveManager.setStoragePool(archive, null);
            archiveSize.decrementAndGet();
          } catch (JsonProcessingException | StorageException e) {
            logger.warn("caught an exception", e);
            throw new InternalErrorThrift().setDetail("fail to set the archive=" + archive);
          }
        } catch (SegmentUnitBecomeBrokenException e) {
          logger.warn("caught an exception", e);
          throw new InternalErrorThrift().setDetail("fail to set the archive=" + archive);
        } catch (Exception e) {
          logger.warn("", e);
          throw new InternalErrorThrift().setDetail(e.getMessage());
        } finally {
          try {
            archiveManager.setStoragePool(archive, null);
          } catch (Exception e) {
            logger.warn("", e);
          }
        }

      }
    }

    if (archiveSize.get() == 0) {
      instanceDomain.setDomainId(null);
    }

    context.setInstanceDomain(instanceDomain);
    return response;
  }

  @Override
  public SetArchiveStoragePoolResponseThrift setArchiveStoragePool(
      SetArchiveStoragePoolRequestThrift request)
      throws ServiceHavingBeenShutdownThrift, InvalidInputExceptionThrift,
      ArchiveIsUsingExceptionThrift,
      TException {
    logger.debug(request.toString());
    if (shutdown) {
      logger.error("datanode:{} had been shutdown", context.getMainEndPoint());
      throw new ServiceHavingBeenShutdownThrift();
    }
    if (pauseRequestProcessing) {
      throw new ConnectionRefusedExceptionThrift();
    }
    if (!request.isSetArchiveIdMapStoragePoolId()) {
      logger.error("Invalid request:{}", request);
      throw new InvalidInputExceptionThrift();
    }
    SetArchiveStoragePoolResponseThrift response = new SetArchiveStoragePoolResponseThrift();

    for (Entry<Long, Long> entry : request.getArchiveIdMapStoragePoolId().entrySet()) {
      Long archiveId = entry.getKey();
      Long storagePoolId = entry.getValue();
      Archive archiveBase = archiveManager.getRawArchive(archiveId);
      if (archiveBase == null) {
        logger.warn("can not find archive:{} to process", archiveId);
        continue;
      }
      RawArchive archive = (RawArchive) archiveBase;
      if (archive.getStoragePoolId() != null) {
        logger.error(
            "archive:{} is using storage pool id:{}, but try change with new storage pool id:{}",
            archiveId, archive.getStoragePoolId(), storagePoolId);
        throw new ArchiveIsUsingExceptionThrift();
      } else {
        try {
          archiveManager.setStoragePool(archive, storagePoolId);
        } catch (JsonProcessingException | StorageException e) {
          logger.warn("caught an exception", e);
          throw new InternalErrorThrift().setDetail("fail to set the archive=" + archive);
        }
      }
    }

    response.setRequestId(request.getRequestId());
    return response;
  }

  @Override
  public FreeArchiveStoragePoolResponseThrift freeArchiveStoragePool(
      FreeArchiveStoragePoolRequestThrift request)
      throws ServiceHavingBeenShutdownThrift, InvalidInputExceptionThrift, TException {
    logger.warn(request.toString());
    if (shutdown) {
      logger.error("datanode:{} had been shutdown", context.getMainEndPoint());
      throw new ServiceHavingBeenShutdownThrift();
    }
    if (pauseRequestProcessing) {
      throw new ConnectionRefusedExceptionThrift();
    }
    FreeArchiveStoragePoolResponseThrift response = new FreeArchiveStoragePoolResponseThrift();
    if (!request.isSetFreeArchiveList()) {
      logger.error("Invalid request:{}", request);
      throw new InvalidInputExceptionThrift();
    }
    for (Long archiveId : request.getFreeArchiveList()) {
      Archive archiveBase = archiveManager.getRawArchive(archiveId);
      if (archiveBase == null) {
        logger.warn("can not find archive:{} to process", archiveId);
        continue;
      }
      RawArchive archive = (RawArchive) archiveBase;
      if (archive.getStoragePoolId() == null) {
        logger.info("archive:{} has already been free", archiveId);
        continue;
      } else {
        try {
          archiveManager.freeArchiveWithOutCheck(archive);
        } catch (AllSegmentUnitDeadInArchviveException de) {
          logger.warn("all segmentUnit in archive {}is dead", archive);
          try {
            archiveManager.setStoragePool(archive, null);
          } catch (JsonProcessingException | StorageException e) {
            logger.warn("caught an exception", e);
            throw new InternalErrorThrift().setDetail("fail to set the archive=" + archive);
          }
        } catch (SegmentUnitBecomeBrokenException e) {
          logger.warn("caught an exception", e);
          throw new InternalErrorThrift().setDetail("fail to set the archive=" + archive);
        } finally {
          try {
            archiveManager.setStoragePool(archive, null);
          } catch (Exception e) {
            logger.warn("", e);
          }
        }
      }
    }

    response.setRequestId(request.getRequestId());
    logger.warn(response.toString());
    return response;
  }

  @Override
  public InitiateCopyPageResponseThrift initiateCopyPage(InitiateCopyPageRequestThrift request)
      throws SegmentNotFoundExceptionThrift, py.thrift.share.NotPrimaryExceptionThrift,
      py.thrift.share.StaleMembershipExceptionThrift,
      py.thrift.share.InvalidMembershipExceptionThrift,
      py.thrift.share.ServiceHavingBeenShutdownThrift,
      py.thrift.share.YouAreNotInMembershipExceptionThrift,
      WrongPrePrimarySessionExceptionThrift, PrimaryIsRollingBackExceptionThrift,
      ConnectionRefusedExceptionThrift {
    logger.info("PreSecondary push data request : {} ", request);

    if (shutdown) {
      throw new ServiceHavingBeenShutdownThrift();
    }

    if (pauseRequestProcessing) {
      throw new ConnectionRefusedExceptionThrift();
    }

    SegId segId = new SegId(request.getVolumeId(), request.getSegIndex());
    SegmentUnit segmentUnit = segmentUnitManager.get(segId);

    Validator.validateInitiateCopyPageInput(request, segmentUnit, context);
    SegmentLogMetadata segmentLogMetadata = segmentUnit.getSegmentLogMetadata();
    long lalId = segmentLogMetadata.getLalId();

    InitiateCopyPageResponseThrift response = new InitiateCopyPageResponseThrift();
    response.setRequestId(request.getRequestId());
    response.setSessionId(request.getSessionId());

    if (lalId < request.getCatchUpLogId()) {
      logger.info("primary plal id={}, my catch up log={}", lalId, request.getCatchUpLogId());
      response.setCanStart(false);
      response.setWaitTimeToRetry(1000);
      return response;
    }

    Instance secondary = instanceStore.get(new InstanceId(request.getMyself()));
    PrimaryCopyPageManager primaryCopyPageManager = segmentUnit.getPrimaryCopyPageManager();
    if (primaryCopyPageManager != null) {
      logger
          .warn("other seg is copying page {}, please wait {}", primaryCopyPageManager, secondary);
      response.setCanStart(false);
      return response;
    }
    Pair<Boolean, Integer> resultPair = ioThrottleManager.register(segmentUnit.getArchive(), segId,
        0, request.getSessionId());
    if (resultPair.getFirst()) {
      primaryCopyPageManager = new PrimaryCopyPageManagerImpl(segmentUnit,
          cfg.getPageCountInRawChunk(),
          request.getSessionId(), secondary, pageManager, ioClientFactory);
      primaryCopyPageManager.setIoThrottleManager(ioThrottleManager);
      for (int j = 0; j < cfg.getConcurrentCopyPageTaskCount(); j++) {
        PrimaryCopyPageTask copyPageTask = new PrimaryCopyPageTask(j, segmentUnit,
            ioService.getCopyEngine(),
            primaryCopyPageManager);
        logger
            .warn("success to register a new push data session {} maxSnapId {}, archive {} task={}",
                request.getSessionId(), request.getMaxSnapshotId(), segmentUnit.getArchive(),
                copyPageTask);
        ioService.getCopyEngine().submit(copyPageTask);
      }
      response.setCanStart(true);
    } else {
      response.setCanStart(false);
      response.setWaitTimeToRetry(resultPair.getSecond());
    }

    return response;
  }

  @Override

  public ReleaseAllLogsResponse releaseAllLogs(ReleaseAllLogsRequest request) throws TException {
    long magicNumberForReleaseLogs = 0x042028;
    if (request.getMagicNumber() != magicNumberForReleaseLogs) {
      throw new TException();
    }

    for (SegmentUnit segmentUnit : segmentUnitManager.get()) {
      SegmentLogMetadata logMetadata = segmentUnit.getSegmentLogMetadata();
      logMetadata.removeAllLog(false);
    }
    return new ReleaseAllLogsResponse();
  }

  @Override
  public GetDbInfoResponseThrift getDbInfo(GetDbInfoRequestThrift request)
      throws py.thrift.share.ServiceHavingBeenShutdownThrift {
    GetDbInfoResponseThrift dbInfoResponse = new GetDbInfoResponseThrift();
    dbInfoResponse.setRequestId(request.getRequestId());
    EndPoint endPoint = context.getEndPoints().get(PortType.CONTROL);
    Group group = context.getGroup();
    long instanceId = context.getInstanceId().getId();
    ReportDbRequestThrift reportDbRequest = this.backupDbReporter
        .buildReportDbRequest(endPoint, group, instanceId, true);
    dbInfoResponse.setDbInfo(reportDbRequest);

    return dbInfoResponse;
  }

  private CheckPrimaryReachableResponse checkPrimaryOrTempPrimaryReachable(
      SegIdThrift segIdThrift,
      SegmentMembershipThrift membership, Broadcastlog.RequestOption requestOption)
      throws SegmentNotFoundExceptionThrift, ServiceHavingBeenShutdownThrift, InternalErrorThrift,
      InvalidSegmentUnitStatusExceptionThrift, TException {
    Broadcastlog.PbCheckRequest.Builder requestBuilder = Broadcastlog.PbCheckRequest.newBuilder();
    requestBuilder.setRequestId(RequestIdBuilder.get());
    requestBuilder.setRequestOption(requestOption);
    requestBuilder.setVolumeId(segIdThrift.getVolumeId());
    requestBuilder.setSegIndex(segIdThrift.getSegmentIndex());
    requestBuilder.setCheckInstanceId(requestOption == Broadcastlog.RequestOption.CHECK_PRIMARY
        ? membership.getPrimary() :
        membership.getTempPrimary());
    requestBuilder.setRequestPbMembership(PbRequestResponseHelper
        .buildPbMembershipFrom(
            RequestResponseHelper.buildSegmentMembershipFrom(membership).getSecond()));
    SettableFuture<Object> future = SettableFuture.create();
    ioService
        .check(requestBuilder.build(), new AbstractMethodCallback<Broadcastlog.PbCheckResponse>() {
          @Override
          public void complete(Broadcastlog.PbCheckResponse object) {
            future.set(object);
          }

          @Override
          public void fail(Exception e) {
            future.set(e);
          }
        });

    try {
      Object result = future.get();
      if (result instanceof Broadcastlog.PbCheckResponse) {
        Broadcastlog.PbCheckResponse pbResponse = (Broadcastlog.PbCheckResponse) result;
        return new CheckPrimaryReachableResponse(pbResponse.getReachable(), pbResponse.getPcl());
      } else {
        throw new InternalErrorThrift().setDetail(result.toString());
      }
    } catch (Exception e) {
      logger.error("exception caught", e);
      throw new InternalErrorThrift().setDetail(e.toString());
    }
  }

  @Override
  public CheckPrimaryReachableResponse checkPrimaryReachable(SegIdThrift segIdThrift,
      SegmentMembershipThrift membership)
      throws TException {
    return checkPrimaryOrTempPrimaryReachable(segIdThrift, membership,
        Broadcastlog.RequestOption.CHECK_PRIMARY);
  }

  public CheckPrimaryReachableResponse checkTempPrimaryReachable(SegIdThrift segIdThrift,
      SegmentMembershipThrift membership)
      throws TException {
    return checkPrimaryOrTempPrimaryReachable(segIdThrift, membership,
        Broadcastlog.RequestOption.CHECK_TEMP_PRIMARY);
  }

  private ConfirmPrimaryUnreachableResponse confirmPrimaryOrTempPrimaryUnreachable(
      ConfirmPrimaryUnreachableRequest request, Broadcastlog.RequestOption requestOption)
      throws TException {
    Broadcastlog.PbCheckRequest.Builder requestBuilder = Broadcastlog.PbCheckRequest.newBuilder();
    requestBuilder.setRequestId(RequestIdBuilder.get());
    switch (requestOption) {
      case CONFIRM_TP_UNREACHABLE:
        requestBuilder.setCheckInstanceId(request.getMembership().getTempPrimary());
        break;
      case CONFIRM_UNREACHABLE:
        requestBuilder.setCheckInstanceId(request.getMembership().getPrimary());
        break;
      default:
        throw new IllegalArgumentException();
    }

    requestBuilder.setRequestOption(requestOption);
    requestBuilder.setVolumeId(request.getSegId().getVolumeId());
    requestBuilder.setSegIndex(request.getSegId().getSegmentIndex());
    requestBuilder.setTempPrimary(request.getTempPrimary());
    requestBuilder.setRequestPbMembership(PbRequestResponseHelper.buildPbMembershipFrom(
        RequestResponseHelper.buildSegmentMembershipFrom((request.getMembership())).getSecond()));
    SettableFuture<Object> future = SettableFuture.create();
    ioService
        .check(requestBuilder.build(), new AbstractMethodCallback<Broadcastlog.PbCheckResponse>() {
          @Override
          public void complete(Broadcastlog.PbCheckResponse object) {
            future.set(object);
          }

          @Override
          public void fail(Exception e) {
            future.set(e);
          }
        });

    try {
      Object result = future.get();
      if (result instanceof Broadcastlog.PbCheckResponse) {
        Broadcastlog.PbCheckResponse checkResponse = (Broadcastlog.PbCheckResponse) result;
        ConfirmPrimaryUnreachableResponse confirmPrimaryUnreachableResponse =
            new ConfirmPrimaryUnreachableResponse(
                1L);
        if (checkResponse.hasPbMembership()) {
          confirmPrimaryUnreachableResponse.setNewMembership(RequestResponseHelper
              .buildThriftMembershipFrom(RequestResponseHelper.buildSegIdFrom(request.getSegId()),
                  PbRequestResponseHelper.buildMembershipFrom(checkResponse.getPbMembership())));
        }
        return confirmPrimaryUnreachableResponse;
      } else {
        throw new InternalErrorThrift().setDetail(result.toString());
      }
    } catch (Exception e) {
      logger.error("exception caught", e);
      throw new InternalErrorThrift().setDetail(e.toString());
    }
  }

  @Override
  public ConfirmPrimaryUnreachableResponse confirmPrimaryUnreachable(
      ConfirmPrimaryUnreachableRequest request)
      throws ServiceHavingBeenShutdownThrift, SegmentNotFoundExceptionThrift, InternalErrorThrift,
      InvalidSegmentUnitStatusExceptionThrift, TException {
    return confirmPrimaryOrTempPrimaryUnreachable(request,
        Broadcastlog.RequestOption.CONFIRM_UNREACHABLE);
  }

  public ConfirmPrimaryUnreachableResponse confirmTempPrimaryUnreachable(
      ConfirmPrimaryUnreachableRequest request)
      throws ServiceHavingBeenShutdownThrift, SegmentNotFoundExceptionThrift, InternalErrorThrift,
      InvalidSegmentUnitStatusExceptionThrift, TException {
    return confirmPrimaryOrTempPrimaryUnreachable(request,
        Broadcastlog.RequestOption.CONFIRM_TP_UNREACHABLE);
  }

  private CheckSecondaryReachableResponse porTpCheckSecondaryReachable(
      CheckSecondaryReachableRequest request,
      Broadcastlog.RequestOption requestOption)
      throws SegmentNotFoundExceptionThrift, NotPrimaryExceptionThrift,
      ServiceHavingBeenShutdownThrift,
      InternalErrorThrift, TException {
    Broadcastlog.PbCheckRequest.Builder requestBuilder = Broadcastlog.PbCheckRequest.newBuilder();
    requestBuilder.setRequestId(RequestIdBuilder.get());
    requestBuilder.setCheckInstanceId(request.getSecondaryInstanceId());
    requestBuilder.setRequestOption(requestOption);
    requestBuilder.setVolumeId(request.getSegId().getVolumeId());
    requestBuilder.setSegIndex(request.getSegId().getSegmentIndex());
    requestBuilder.setRequestPbMembership(PbRequestResponseHelper.buildPbMembershipFrom(
        RequestResponseHelper.buildSegmentMembershipFrom(request.getMembership()).getSecond()));
    SettableFuture<Object> future = SettableFuture.create();
    ioService
        .check(requestBuilder.build(), new AbstractMethodCallback<Broadcastlog.PbCheckResponse>() {
          @Override
          public void complete(Broadcastlog.PbCheckResponse object) {
            future.set(object);
          }

          @Override
          public void fail(Exception e) {
            future.set(e);
          }
        });

    try {
      Object result = future.get();
      if (result instanceof Broadcastlog.PbCheckResponse) {
        Broadcastlog.PbCheckResponse pbCheckResponse = (Broadcastlog.PbCheckResponse) result;
        CheckSecondaryReachableResponse response = new CheckSecondaryReachableResponse(
            request.getRequestId(),
            pbCheckResponse.getReachable());
        if (pbCheckResponse.hasPbMembership()) {
          response.setNewMembership(RequestResponseHelper
              .buildThriftMembershipFrom(RequestResponseHelper.buildSegIdFrom(request.getSegId()),
                  PbRequestResponseHelper.buildMembershipFrom(pbCheckResponse.getPbMembership())));
        }
        return response;
      } else {
        throw new InternalErrorThrift().setDetail(result.toString());
      }
    } catch (Exception e) {
      logger.error("exception caught", e);
      throw new InternalErrorThrift().setDetail(e.toString());
    }
  }

  @Override
  public CheckSecondaryReachableResponse checkSecondaryReachable(
      CheckSecondaryReachableRequest request)
      throws SegmentNotFoundExceptionThrift, NotPrimaryExceptionThrift,
      ServiceHavingBeenShutdownThrift,
      InternalErrorThrift, TException {
    return porTpCheckSecondaryReachable(request, Broadcastlog.RequestOption.CHECK_SECONDARY);
  }

  public CheckSecondaryReachableResponse tpCheckSecondaryReachable(
      CheckSecondaryReachableRequest request)
      throws SegmentNotFoundExceptionThrift, NotPrimaryExceptionThrift,
      ServiceHavingBeenShutdownThrift,
      InternalErrorThrift, TException {
    return porTpCheckSecondaryReachable(request, Broadcastlog.RequestOption.TP_CHECK_SECONDARY);
  }

  @Override
  public RetrieveBitMapResponse retrieveBitMap(RetrieveBitMapRequest request)
      throws ServiceHavingBeenShutdownThrift, SegmentNotFoundExceptionThrift,
      OnSameArchiveExceptionThrift,
      InternalErrorThrift, TException {
    if (shutdown) {
      throw new ServiceHavingBeenShutdownThrift();
    }
    if (pauseRequestProcessing) {
      throw new ConnectionRefusedExceptionThrift();
    }

    SegId segId = RequestResponseHelper.buildSegIdFrom(request.getSegId());
    SegmentUnit segmentUnit = segmentUnitManager.get(segId);
    if (segmentUnit == null) {
      throw new SegmentNotFoundExceptionThrift();
    }

    SegmentUnitStatus status = segmentUnit.getSegmentUnitMetadata().getStatus();
    if (status.hasGone()) {
      throw new SegmentNotFoundExceptionThrift();
    }

    if (!request.isOnSameArchive()) {
      if (request.getMyInstanceId() == context.getInstanceId().getId()
          && request.getArchiveId() == segmentUnit
          .getArchive().getArchiveId()) {
        logger.warn("we are on the same archive");
        throw new OnSameArchiveExceptionThrift().setDetail("we are on the same archive");
      }
    }

    logger.warn("{} retrieve bitmap request got, cardinality {}", segmentUnit.getSegId(),
        segmentUnit.getSegmentUnitMetadata().getBitmap().cardinality(Data));
    byte[] bitMapInBytes = segmentUnit.getSegmentUnitMetadata().getBitmap().toByteArray(Data);
    RetrieveBitMapResponse response = new RetrieveBitMapResponse();
    response.setRequestId(request.getRequestId());
    response.setBitMapInBytes(bitMapInBytes);
    response.setLatestMembrship(RequestResponseHelper
        .buildThriftMembershipFrom(segId, segmentUnit.getSegmentUnitMetadata().getMembership()));
    return response;
  }

  @Override
  public InnerMigrateSegmentUnitResponse innerMigrateSegmentUnit(
      InnerMigrateSegmentUnitRequest request)
      throws ServiceHavingBeenShutdownThrift, SegmentNotFoundExceptionThrift, InternalErrorThrift,
      SnapshotRollingBackExceptionThrift, SegmentUnitCloningExceptionThrift,
      ArchiveNotFoundExceptionThrift,
      NotEnoughSpaceExceptionThrift, TException {
    throw new InternalErrorThrift().setDetail("unsupported operation");
  }

  @Override
  public BackupDatabaseInfoResponse backupDatabaseInfo(BackupDatabaseInfoRequest request)
      throws ServiceHavingBeenShutdownThrift, TException {
    if (shutdown) {
      throw new ServiceHavingBeenShutdownThrift();
    }

    backupDbReporter.processRsp(request.getDatabaseInfo());
    return new BackupDatabaseInfoResponse(request.getRequestId());
  }

  @Override
  public MigratePrimaryResponse migratePrimary(MigratePrimaryRequest request)
      throws ServiceHavingBeenShutdownThrift, SegmentNotFoundExceptionThrift, InternalErrorThrift,
      PrimaryCandidateCantBePrimaryExceptionThrift, TException {
    if (shutdown) {
      throw new ServiceHavingBeenShutdownThrift();
    }
    if (pauseRequestProcessing) {
      throw new ConnectionRefusedExceptionThrift();
    }

    SegId segId = RequestResponseHelper.buildSegIdFrom(request.getSegId());
    InstanceId primaryCandidate = new InstanceId(request.getPrimaryCandidate());

    logger.warn("migrate primary request {}", request);

    innerMigratePrimary(segId, primaryCandidate);
    return new MigratePrimaryResponse(request.getRequestId());
  }

  @Override
  public CanYouBecomePrimaryResponse canYouBecomePrimary(CanYouBecomePrimaryRequest request)
      throws ServiceHavingBeenShutdownThrift, SegmentNotFoundExceptionThrift,
      InternalErrorThrift, TException {
    if (shutdown) {
      throw new ServiceHavingBeenShutdownThrift();
    }
    if (pauseRequestProcessing) {
      throw new ConnectionRefusedExceptionThrift();
    }

    SegmentUnit segmentUnit = segmentUnitManager
        .get(RequestResponseHelper.buildSegIdFrom(request.getSegId()));
    if (segmentUnit == null) {
      throw new SegmentNotFoundExceptionThrift();
    }

    SegmentUnitStatus segmentUnitStatus = segmentUnit.getSegmentUnitMetadata().getStatus();
    SegmentMembership membership = segmentUnit.getSegmentUnitMetadata().getMembership();
    if (segmentUnitStatus != SegmentUnitStatus.Secondary || !membership
        .isSecondary(context.getInstanceId())) {
      return new CanYouBecomePrimaryResponse(request.getRequestId(), false);
    }

    SegmentMembership requestMembership = RequestResponseHelper
        .buildSegmentMembershipFrom(request.getMembership())
        .getSecond();
    if (membership.compareTo(requestMembership) != 0) {
      return new CanYouBecomePrimaryResponse(request.getRequestId(), false);
    }

    ArchiveStatus archiveStatus = segmentUnit.getArchive().getArchiveStatus();
    if (archiveStatus != ArchiveStatus.GOOD) {
      return new CanYouBecomePrimaryResponse(request.getRequestId(), false);
    }

    return new CanYouBecomePrimaryResponse(request.getRequestId(), true);
  }

  @Override
  public ImReadyToBePrimaryResponse iAmReadyToBePrimary(ImReadyToBePrimaryRequest request)
      throws ServiceHavingBeenShutdownThrift, SegmentNotFoundExceptionThrift,
      YouAreNotInMembershipExceptionThrift, YouAreNotReadyExceptionThrift,
      InternalErrorThrift, TException {
    if (shutdown) {
      throw new ServiceHavingBeenShutdownThrift();
    }
    if (pauseRequestProcessing) {
      throw new ConnectionRefusedExceptionThrift();
    }

    SegId segId = RequestResponseHelper.buildSegIdFrom(request.getSegId());
    SegmentUnit segmentUnit = segmentUnitManager.get(segId);
    if (segmentUnit == null) {
      throw new SegmentNotFoundExceptionThrift();
    }

    Validator.validateImReadyToBePrimaryInput(request, segmentUnit, context);
    SegmentLogMetadata logMetadata = segmentUnit.getSegmentLogMetadata();

    if (!logMetadata.freezeClInPrimary(request.getMyClId())) {
      throw new YouAreNotReadyExceptionThrift();
    }

    SegmentMembership newMembership;
    try {
      SegmentMembership membership = segmentUnit.getSegmentUnitMetadata().getMembership();
      newMembership = membership.primaryCandidateBecomePrimary(new InstanceId(request.getMyself()));
      if (newMembership == null) {
        logger.warn("the request primary candidate {} is not in right position of {}",
            request.getMyself(),
            membership);
        throw new YouAreNotInRightPositionExceptionThrift();
      }

      try {
        archiveManager.updateSegmentUnitMetadata(segId, newMembership, SegmentUnitStatus.Start);
      } catch (Exception e) {
        logger.warn("cannot update membership", e);
        throw new InternalErrorThrift().setDetail("can not update membership");
      }
    } finally {
      logMetadata.resetFreezeClInPrimary();
    }

    try {
      dataNodeAsyncClient.broadcast(DataNodeRequestResponseHelper
              .buildPrimaryChangedRequest(RequestIdBuilder.get(), segId, newMembership),
          cfg.getDataNodeRequestTimeoutMs(),
          segmentUnit.getSegmentUnitMetadata().getVolumeType().getVotingQuorumSize() - 1,
          true, RequestResponseHelper
              .buildEndPoints(instanceStore, newMembership, true,
                  context.getInstanceId()));
    } catch (QuorumNotFoundException | FailedToSendBroadcastRequestsException
        | SnapshotVersionMissMatchForMergeLogsException e) {
      logger.warn("broadcast primary changed failed", e);

    }

    return new ImReadyToBePrimaryResponse(request.getRequestId(),
        RequestResponseHelper.buildThriftMembershipFrom(segId, newMembership));
  }

  private void innerMigratePrimary(SegId segId, InstanceId primaryCandidate)
      throws PrimaryCandidateCantBePrimaryExceptionThrift, SegmentNotFoundExceptionThrift,
      InternalErrorThrift {
    SegmentUnit segmentUnit = segmentUnitManager.get(segId);
    if (segmentUnit == null) {
      throw new SegmentNotFoundExceptionThrift();
    }

    SegmentMembership membership = segmentUnit.getSegmentUnitMetadata().getMembership();
    if (membership.isPrimaryCandidate(primaryCandidate)) {
      return;
    } else if (!membership.isPrimary(context.getInstanceId())) {
      logger.warn("I am not primary !! {}", segId);
      return;
    } else if (segmentUnit.getSegmentUnitMetadata().getStatus() != SegmentUnitStatus.Primary) {
      logger.warn("my status is not primary but {} !! {}",
          segmentUnit.getSegmentUnitMetadata().getStatus(), segId);
      return;
    } else {
      Instance primaryCandidateInstance = instanceStore.get(primaryCandidate);
      if (primaryCandidateInstance == null) {
        logger.error("can't get primary candidate in instance store {} {}", segId,
            primaryCandidate);
        throw new InternalErrorThrift()
            .setDetail("primary candidate not found in instance store " + segId);
      }
      CanYouBecomePrimaryResponse response;

      try {
        DataNodeService.Iface client = dataNodeSyncClientFactory
            .generateSyncClient(primaryCandidateInstance.getEndPoint());
        response = client
            .canYouBecomePrimary(new CanYouBecomePrimaryRequest(RequestIdBuilder.get(),
                RequestResponseHelper.buildThriftSegIdFrom(segId),
                RequestResponseHelper.buildThriftMembershipFrom(segId, membership)));
      } catch (GenericThriftClientFactoryException e) {
        logger.error("can't generate a client of primary candidate {} {}", segId,
            primaryCandidate);
        throw new InternalErrorThrift()
            .setDetail("can't generate a client of primary candidate " + segId);
      } catch (TException e) {
        logger.error("failed to ask primary candidate if he is able to be primary {} {}",
            segId,
            primaryCandidate, e);
        throw new InternalErrorThrift()
            .setDetail(
                "failed to ask primary candidate if he is able to be primary " + segId);
      }

      if (!response.isEverythingOk()) {
        logger.warn("primary candidate said he can't be primary right now {} {}", segId,
            primaryCandidate);
        throw new PrimaryCandidateCantBePrimaryExceptionThrift()
            .setDeatil("primary candidate said he can't be primary right now " + segId);
      }

      SegmentMembership newMembership = membership
          .secondaryBecomePrimaryCandidate(primaryCandidate);
      if (newMembership == null) {
        throw new InternalErrorThrift()
            .setDetail("the requested primary candidate is not secondary in membership");
      } else {
        try {
          archiveManager.updateSegmentUnitMetadata(segId, newMembership, null);

          MembershipPusher.getInstance(context.getInstanceId()).submit(segId);
        } catch (Exception e) {
          logger.error("cannot update membership", e);
          throw new InternalErrorThrift().setDetail("can not update membership");
        }
      }
    }
  }

  @Override
  public DepartFromMembershipRespone departFromMembership(DepartFromMembershipRequest request)
      throws SegmentNotFoundExceptionThrift, RefuseDepartFromMembershipExceptionThrift,
      ServiceHavingBeenShutdownThrift, ConnectionRefusedExceptionThrift, TException {
    logger.warn("departFormMembership request :{}", request);
    if (shutdown) {
      throw new ServiceHavingBeenShutdownThrift();
    }
    if (pauseRequestProcessing) {
      throw new ConnectionRefusedExceptionThrift();
    }

    SegId segId = new SegId(request.getVolumeId(), request.getSegIndex());
    SegmentUnit segUnit = segmentUnitManager.get(segId);
    SegmentMembership membership = segUnit.getSegmentUnitMetadata().getMembership();
    boolean synPersist = request.isSynPersist();
    int votingCount = segUnit.getSegmentUnitMetadata().getVolumeType().getVotingQuorumSize();

    long departInstanceId = request.getInstanceId();
    if (segUnit.getSegmentUnitMetadata().getStatus() != SegmentUnitStatus.Primary) {
      logger.warn("I am not primary");
      throw new RefuseDepartFromMembershipExceptionThrift();
    }

    if (!segUnit.tryLockPeerDeparting()) {
      logger.warn("some one else is departing");
      throw new RefuseDepartFromMembershipExceptionThrift();
    }

    try {
      if (membership.getPrimary().getId() == departInstanceId) {
        boolean membershipStable = true;

        SegmentMembership requestMembership = RequestResponseHelper
            .buildSegmentMembershipFrom(request.getMembership()).getSecond();
        if (!requestMembership.equals(membership)) {
          logger.warn("the request membership is {}, the primary membership is {}",
              requestMembership,
              membership);
          throw new RefuseDepartFromMembershipExceptionThrift();
        }

        if (!isSegmentUnitStable(membership,
            segUnit.getSegmentUnitMetadata().getVolumeType())) {
          logger.warn("the membership {} is not stable ", membership);
          membershipStable = false;
        } else {
          BroadcastRequest broadcastRequest = RequestResponseHelper
              .buildGiveMeYourMembershipRequest(segId, membership,
                  membership.getPrimary().getId());

          try {
            BroadcastResult broadcastResult = dataNodeAsyncClient
                .broadcast(broadcastRequest, cfg.getDataNodeRequestTimeoutMs(),
                    segUnit.getSegmentUnitMetadata().getVolumeType()
                        .getVotingQuorumSize(),
                    true,
                    RequestResponseHelper
                        .buildEndPoints(instanceStore, membership, true,
                            context.getInstanceId()));
            for (Entry<EndPoint, BroadcastResponse> endPointAndResponse : broadcastResult
                .getGoodResponses()
                .entrySet()) {
              BroadcastResponse peerResponse = endPointAndResponse.getValue();
              EndPoint peerEndPoint = endPointAndResponse.getKey();
              SegmentUnitStatus peerStatus = SegmentUnitStatus
                  .valueOf(peerResponse.getMyStatus().name());
              if (peerStatus != SegmentUnitStatus.Arbiter
                  && peerStatus != SegmentUnitStatus.Secondary) {
                logger
                    .warn("the current status for {} is {}", peerEndPoint,
                        peerStatus);
                membershipStable = false;
                break;
              }

              SegmentMembership peerMembership = RequestResponseHelper
                  .buildSegmentMembershipFrom(peerResponse.getMembership())
                  .getSecond();

              if (!membership.equals(peerMembership)) {
                logger.warn(
                    "peer membership of {} is {}, while the primary membership is {}",
                    peerEndPoint,
                    peerMembership, membership);
                membershipStable = false;
                break;
              }
            }
          } catch (QuorumNotFoundException | FailedToSendBroadcastRequestsException
              | SnapshotVersionMissMatchForMergeLogsException e) {
            logger.warn("broadcast failed", e);
            membershipStable = false;
          }

        }

        if (!membershipStable) {
          Validate.isTrue(context.getInstanceId().getId() == request.getInstanceId());
          if (!membership.getSecondaries().isEmpty()) {
            innerMigratePrimary(segId, membership.getSecondaries().iterator().next());
          }

          throw new RefuseDepartFromMembershipExceptionThrift();
        }
      } else if (membership.contain(departInstanceId)) {
        SegmentMembership requestMembership = RequestResponseHelper
            .buildSegmentMembershipFrom(request.getMembership()).getSecond();
        if (!requestMembership.equals(membership)) {
          logger.warn("the request membership is {}, the primary membership is {}",
              requestMembership,
              membership);
          throw new RefuseDepartFromMembershipExceptionThrift();
        }

        if (membership.getAliveSecondaries().size() + 1 == votingCount) {
          if (!membership.getInactiveSecondaries()
              .contains(new InstanceId(departInstanceId))) {
            logger.warn("the memship {} can not depart instance {}",
                membership, departInstanceId);
            throw new RefuseDepartFromMembershipExceptionThrift();
          }
        } else if (membership.getAliveSecondaries().size() + 1 < votingCount) {
          logger.warn("the memship {} can not depart instance {}",
              membership, departInstanceId);
          throw new RefuseDepartFromMembershipExceptionThrift();
        }

        int successCount = 1;
        boolean canDepart = true;
        BroadcastRequest broadcastRequest = RequestResponseHelper
            .buildGiveMeYourMembershipRequest(segId, membership,
                membership.getPrimary().getId());

        try {
          BroadcastResult broadcastResult = dataNodeAsyncClient
              .broadcast(broadcastRequest, cfg.getDataNodeRequestTimeoutMs(),
                  segUnit.getSegmentUnitMetadata().getVolumeType().getVotingQuorumSize(),
                  true,
                  RequestResponseHelper
                      .buildEndPoints(instanceStore, membership, true,
                          context.getInstanceId()));
          for (Entry<EndPoint, BroadcastResponse> endPointAndResponse : broadcastResult
              .getGoodResponses()
              .entrySet()) {
            BroadcastResponse peerResponse = endPointAndResponse.getValue();
            EndPoint peerEndPoint = endPointAndResponse.getKey();

            SegmentMembership peerMembership = RequestResponseHelper
                .buildSegmentMembershipFrom(peerResponse.getMembership()).getSecond();

            if (!membership.equals(peerMembership)) {
              logger.warn(
                  "peer membership of {} is {}, while the primary membership is {}",
                  peerEndPoint,
                  peerMembership, membership);
              continue;
            }
            successCount++;
          }
        } catch (QuorumNotFoundException | FailedToSendBroadcastRequestsException
            | SnapshotVersionMissMatchForMergeLogsException e) {
          logger.warn("broadcast failed", e);
          canDepart = false;
        }

        if (successCount <= votingCount) {
          canDepart = false;
        }

        if (!canDepart) {
          logger.warn("the request membership is {}, the primary membership is {}",
              requestMembership,
              membership);
          throw new RefuseDepartFromMembershipExceptionThrift();
        }

      } else {
        SegmentMembership requestMembership = RequestResponseHelper
            .buildSegmentMembershipFrom(request.getMembership()).getSecond();
        int successCount = 1;
        boolean canDepart = true;
        BroadcastRequest broadcastRequest = RequestResponseHelper
            .buildGiveMeYourMembershipRequest(segId, membership,
                membership.getPrimary().getId());

        try {
          BroadcastResult broadcastResult = dataNodeAsyncClient
              .broadcast(broadcastRequest, cfg.getDataNodeRequestTimeoutMs(),
                  segUnit.getSegmentUnitMetadata().getVolumeType().getVotingQuorumSize(),
                  true,
                  RequestResponseHelper
                      .buildEndPoints(instanceStore, membership, true,
                          context.getInstanceId()));
          for (Entry<EndPoint, BroadcastResponse> endPointAndResponse : broadcastResult
              .getGoodResponses()
              .entrySet()) {
            BroadcastResponse peerResponse = endPointAndResponse.getValue();
            EndPoint peerEndPoint = endPointAndResponse.getKey();

            SegmentMembership peerMembership = RequestResponseHelper
                .buildSegmentMembershipFrom(peerResponse.getMembership()).getSecond();

            if (!membership.equals(peerMembership)) {
              logger.warn(
                  "peer membership of {} is {}, while the primary membership is {}",
                  peerEndPoint,
                  peerMembership, membership);
              continue;
            }
            successCount++;
          }
        } catch (QuorumNotFoundException | FailedToSendBroadcastRequestsException
            | SnapshotVersionMissMatchForMergeLogsException e) {
          logger.warn("broadcast failed", e);
          canDepart = false;
        }

        if (successCount < votingCount) {
          canDepart = false;
        }

        if (!canDepart) {
          logger.warn("the request membership is {}, the primary membership is {}",
              requestMembership,
              membership);
          throw new RefuseDepartFromMembershipExceptionThrift();
        }
      }

      Instance peerInstance = this.instanceStore.get(new InstanceId(request.getInstanceId()));
      try {
        DataNodeService.Iface dataNodeClient = dataNodeSyncClientFactory
            .generateSyncClient(peerInstance.getEndPoint());
        DeleteSegmentUnitRequest deleteSegmentRequest = RequestResponseHelper
            .buildDeleteSegmentUnitRequest(segId, membership).setSynPersist(synPersist);
        dataNodeClient.deleteSegmentUnit(deleteSegmentRequest);
      } catch (GenericThriftClientFactoryException e) {
        logger.error("depart from membership error", e);
        throw new RefuseDepartFromMembershipExceptionThrift();
      }

      return new DepartFromMembershipRespone(request.getRequestId());
    } finally {
      segUnit.unlockPeerDeparting();
    }

  }

  @Override
  public SettleArchiveTypeResponse settleArchiveType(SettleArchiveTypeRequest request)
      throws DiskNotFoundExceptionThrift, DiskSizeCanNotSupportArchiveTypesThrift,
      ServiceHavingBeenShutdownThrift, ArchiveManagerNotSupportExceptionThrift,
      InternalErrorThrift {
    logger.warn("set Archive Type request {}", request);
    if (shutdown) {
      throw new ServiceHavingBeenShutdownThrift();
    }

    List<ArchiveTypeThrift> archiveTypeThrifts = request.getArchiveTypes();
    Long archiveId = request.getArchiveId();
    if (archiveTypeThrifts.size() == 0) {
      logger.error("archive  types size is 0");
      throw new DiskSizeCanNotSupportArchiveTypesThrift();
    }

    if (archiveTypeThrifts.size() > 1) {
      throw new ArchiveManagerNotSupportExceptionThrift();
    }

    UnsettledArchive unsettledArchive = (UnsettledArchive) unsettledArchiveManager
        .getArchive(archiveId);
    if (unsettledArchive == null) {
      throw new DiskNotFoundExceptionThrift();
    }

    List<String> archiveTypeList = new ArrayList<>();

    boolean changeType = false;

    List<ArchiveType> originalArchiveTypes = unsettledArchive.getUnsettledArchiveMetadata()
        .getOriginalType();
    if (originalArchiveTypes.size() != archiveTypeThrifts.size()) {
      changeType = true;
    }

    boolean isRawFile = false;
    File file = new File(cfg.getArchiveConfiguration().getPersistRootDir());
    for (ArchiveTypeThrift archiveTypeThrift : archiveTypeThrifts) {
      switch (archiveTypeThrift) {
        case RAW_DISK:
          archiveTypeList.add(cfg.getRawAppName());
          if (!originalArchiveTypes.contains(ArchiveType.RAW_DISK)) {
            changeType = true;
          }
          isRawFile = true;
          break;
        default:
      }
    }

    if (isRawFile) {
      file = new File(file, cfg.getArchiveConfiguration().getDataArchiveDir());
    }

    String oldlinkname = FilenameUtils.getBaseName(unsettledArchive.getStorage().identifier());
    String cmd =
        cfg.getArchiveConfiguration().getMoveArchiveScriptPath() + " " + oldlinkname + " " + String
            .valueOf(changeType) + " " + String.join(" ", archiveTypeList);
    logger.warn("execute cmd {} to check whether all the disks in data node is changed", cmd);

    String filelinkName = cmdRun(cmd);

    file = new File(file, filelinkName);

    logger.warn("the new file is {}", file.getAbsolutePath());

    try {
      Storage newStorage = new PriorityStorageImpl(
          (AsyncStorage) AsynchronousFileChannelStorageFactory.getInstance()
              .generate(file.getAbsolutePath()),
          cfg.getWriteTokenCount(), storageExceptionHandlerChain, hashedWheelTimer,
          cfg.getStorageIoTimeout(), (int) ArchiveOptions.PAGE_PHYSICAL_SIZE);

      unsettledArchiveManager.removeArchive(archiveId);
      AbstractArchiveBuilder builder = null;
      if (isRawFile) {
        builder = new RawArchiveBuilder(cfg, newStorage);
        ((RawArchiveBuilder) builder).setSyncLogTaskExecutor(syncLogTaskExecutor);
      }
      try {
        Archive archive = builder.build();
        pluginPlugoutManager.plugin(archive);
      } catch (Exception e) {
        logger.error("build the raw archive ERROR", e);
        throw new InternalErrorThrift();
      }

    } catch (StorageException e) {
      e.printStackTrace();
    }

    return new SettleArchiveTypeResponse(request.getRequestId());
  }

  @Override
  public StartOnlineMigrationResponse startOnlineMigration(StartOnlineMigrationRequest request)
      throws ServiceHavingBeenShutdownThrift, ConnectionRefusedExceptionThrift,
      SegmentExistingExceptionThrift, InitializeBitmapExceptionThrift {
    logger.warn("start online migration request :{} ", request);

    if (shutdown) {
      logger.warn("service was shutdown.");
      throw new ServiceHavingBeenShutdownThrift();
    }
    if (pauseRequestProcessing) {
      logger.warn("service was paused.");
      throw new ConnectionRefusedExceptionThrift();
    }

    SegId segId = new SegId(request.getVolumeId(), request.getSegIndex());
    SegmentUnit segUnit = segmentUnitManager.get(segId);
    if (segUnit == null) {
      logger.error("segId does not existing. segId:{}", segId);
      throw new SegmentExistingExceptionThrift();
    }

    if (segUnit.getSegmentUnitMetadata().isArbiter()) {
      logger.warn("segUnit:{} is arbiter.", segId);
      throw new InitializeBitmapExceptionThrift();
    }

    if (!segUnit.getSegmentUnitMetadata().isOnlineMigrationSegmentUnit()) {
      logger.error("segUnit:{} is not online migration.");
      throw new InitializeBitmapExceptionThrift();
    }

    StartOnlineMigrationResponse startOnlineMigrationResponse = new StartOnlineMigrationResponse();
    startOnlineMigrationResponse.setRequestId(request.getRequestId());
    startOnlineMigrationResponse.setResult(true);
    logger.warn("startOnlineMigrationResponse:{}", startOnlineMigrationResponse);
    return startOnlineMigrationResponse;
  }

  @Override
  public DegradeDiskResponse degradeDisk(DegradeDiskRequest request) throws TException {
    if (shutdown) {
      logger.warn("service was shutdown.");
      throw new ServiceHavingBeenShutdownThrift();
    }
    if (pauseRequestProcessing) {
      logger.warn("service was paused.");
      throw new ConnectionRefusedExceptionThrift();
    }

    long archiveId = request.getArchiveId();
    RawArchive archive = archiveManager.getRawArchive(archiveId);
    DegradeDiskResponse responce = new DegradeDiskResponse(request.getRequestId());
    if (archive != null) {
      logger.warn("find raw archive {}", archive);
      if (archive.getArchiveStatus() == ArchiveStatus.GOOD) {
        try {
          archive.setArchiveStatus(ArchiveStatus.DEGRADED);
        } catch (Exception e) {
          logger.warn("modify archive {} status error", archive, e);
        }

      }
    }
    return responce;
  }

  @Override
  public ForceUpdateMembershipResponse forceUpdateMembership(
      ForceUpdateMembershipRequest request)
      throws ServiceHavingBeenShutdownThrift, ConnectionRefusedExceptionThrift, TException {
    if (shutdown) {
      logger.warn("service was shutdown.");
      throw new ServiceHavingBeenShutdownThrift();
    }
    if (pauseRequestProcessing) {
      logger.warn("service was paused.");
      throw new ConnectionRefusedExceptionThrift();
    }

    ForceUpdateMembershipResponse response = new ForceUpdateMembershipResponse(
        request.getRequestId());
    SegId segId = new SegId(request.getSegId().getVolumeId(),
        request.getSegId().getSegmentIndex());
    SegmentUnit segmentUnit = segmentUnitManager.get(segId);
    if (segmentUnit == null) {
      logger.error("segId does not existing. segId:{}", segId);
      throw new SegmentExistingExceptionThrift();
    }

    SegmentMembership currentMembership = segmentUnit.getSegmentUnitMetadata().getMembership();
    SegmentMembership newMembership = RequestResponseHelper.buildSegmentMembershipFrom(
        request.getNewMembership()).getSecond();

    if (newMembership.compareTo(currentMembership) > 0) {
      try {
        segmentUnit.getArchive()
            .updateSegmentUnitMetadata(segId, newMembership, SegmentUnitStatus.Start);
      } catch (Exception e) {
        logger.error("update segment unit {} error ", segId, e);
      }
    }
    return response;
  }

  @Override
  public CheckSecondaryHasAllLogResponse checkSecondaryHasAllLog(
      CheckSecondaryHasAllLogRequest request)
      throws ServiceHavingBeenShutdownThrift, ConnectionRefusedExceptionThrift, TException {
    if (shutdown) {
      logger.warn("service was shutdown.");
      throw new ServiceHavingBeenShutdownThrift();
    }
    if (pauseRequestProcessing) {
      logger.warn("service was paused.");
      throw new ConnectionRefusedExceptionThrift();
    }

    SegId segId = new SegId(request.getVolumeId(),
        request.getSegIndex());
    SegmentUnit segmentUnit = segmentUnitManager.get(segId);
    if (segmentUnit == null) {
      logger.error("segId does not existing. segId:{}", segId);
      throw new SegmentExistingExceptionThrift();
    }

    logger.warn("check result {} {} {}",
        segmentUnit.getSegmentLogMetadata().getPrimaryMaxLogIdWhenPsi(),
        segmentUnit.getSegmentLogMetadata().getClId(),
        segmentUnit.getSegmentUnitMetadata().getMissLogWhenPsi());
    return new CheckSecondaryHasAllLogResponse(request.getRequestId(),
        segmentUnit.getSegmentLogMetadata().getPrimaryMaxLogIdWhenPsi()
            <= segmentUnit.getSegmentLogMetadata().getClId());
  }

  @Override
  public KickOffPotentialPrimaryResponse kickoffPrimary(KickOffPotentialPrimaryRequest request)
      throws SegmentNotFoundExceptionThrift, InvalidSegmentUnitStatusExceptionThrift,
      ServiceHavingBeenShutdownThrift, InvalidMembershipExceptionThrift, NotPrimaryExceptionThrift,
      InternalErrorThrift, TException {
    logger.debug("Get a kickoff primary request : {} ", request);

    if (shutdown) {
      throw new ServiceHavingBeenShutdownThrift();
    }
    if (pauseRequestProcessing) {
      throw new ConnectionRefusedExceptionThrift();
    }

    SegId segId = new SegId(request.getVolumeId(), request.getSegIndex());
    SegmentUnit segUnit = segmentUnitManager.get(segId);

    Validator.validateKickoffPrimaryRequest(request, segUnit, context);

    SegmentUnitStatus status = segUnit.getSegmentUnitMetadata().getStatus();
    if (status == SegmentUnitStatus.PrePrimary) {
      logger.debug("The seg {} is at PrePrimary status and received another kickoff request {}. "
          + "Bait out and let the BecomePrimary does its job", segId, request);
      return new KickOffPotentialPrimaryResponse(request.getRequestId());
    }

    if (status != SegmentUnitStatus.ModeratorSelected && status != SegmentUnitStatus.Start) {
      if (status != SegmentUnitStatus.OFFLINING && status != SegmentUnitStatus.Deleting
          && status != SegmentUnitStatus.OFFLINED) {
        logger.warn("{} is not at ModeratorSelected or start status but {}, "
            + "which means I have not participated in this "
            + "voting, I will refuse the kick off request and change to Start", segId, status);
        try {
          archiveManager.updateSegmentUnitMetadata(segId, null, SegmentUnitStatus.Start);
        } catch (Exception e) {
          logger.error("can not update segment unit status to Start", e);
          throw new InternalErrorThrift().setDetail(e.getMessage());
        }
      }
      throw new InvalidSegmentUnitStatusExceptionThrift()
          .setMyStatus(status.getSegmentUnitStatusThrift());
    }

    SegmentMembership receivedSegmentMembership = DataNodeRequestResponseHelper
        .buildSegmentMembershipFrom(request.getLatestMembership()).getSecond();
    try {
      logger.debug("Validated kickoff request. Start to update {} membership to and status "
              + "to PrePrimary {} ",
          segId, receivedSegmentMembership);

      segUnit.setDisallowPrePrimaryPclDriving(true);
      archiveManager.updateSegmentUnitMetadata(segId, receivedSegmentMembership,
          SegmentUnitStatus.PrePrimary);
      segUnit.clearMyLease();
      segUnit.clearPeerLeases();

      BecomePrimary becomePrimary;
      if (segUnit.getSegmentUnitMetadata().getVolumeType() == VolumeType.LARGE) {
        becomePrimary = new BecomePrimary(segUnit, receivedSegmentMembership, cfg,
            mutationLogManager, instanceStore, archiveManager, catchupLogEngine, plalEngine,
            dataNodeAsyncClient, context.getInstanceId(), request.getRequestId(),
            request.getPriority(), logStorageReader);
      } else {
        becomePrimary = new BecomePrimary(segUnit, receivedSegmentMembership, cfg,
            mutationLogManager, instanceStore, archiveManager, catchupLogEngine, plalEngine,
            dataNodeAsyncClient, context.getInstanceId(), request.getRequestId(),
            request.getPriority(), logStorageReader);
      }

      becomePrimaryExecutor.execute(becomePrimary);
      KickOffPotentialPrimaryResponse response =
          new KickOffPotentialPrimaryResponse(request.getRequestId());
      logger.debug("Ready to return kickoff response {}", response);
      return response;
    } catch (Throwable e) {
      logger.error("caught an exception when kicking off a primary. "
              + "Trying to change the segment status to Start",
          e);
      try {
        archiveManager.updateSegmentUnitMetadata(segId, null, SegmentUnitStatus.Start);
      } catch (Exception e1) {
        logger.error(
            "Caught an exception while updating segment unit metadata within try-catch block."
                + " Nothing we can do about it. shutdown the service",
            e);
        shutdown();
      }
      InternalErrorThrift ie = new InternalErrorThrift();
      ie.setDetail(e.getMessage());
      throw ie;
    }
  }

  @Override
  public MakePrimaryDecisionResponse makePrimaryDecision(MakePrimaryDecisionRequest request)
      throws SegmentNotFoundExceptionThrift, InvalidMembershipExceptionThrift,
      NotPrimaryExceptionThrift, ServiceHavingBeenShutdownThrift,
      InvalidSegmentUnitStatusExceptionThrift,
      FailedToKickOffPrimaryExceptionThrift, NoAvailableCandidatesForNewPrimaryExceptionThrift,
      InternalErrorThrift, TException {
    logger.debug("Get a make primary decision request : {}", request);
    if (shutdown) {
      throw new ServiceHavingBeenShutdownThrift();
    }
    if (pauseRequestProcessing) {
      throw new ConnectionRefusedExceptionThrift();
    }

    SegId segId = new SegId(request.getVolumeId(), request.getSegIndex());
    SegmentUnit segUnit = segmentUnitManager.get(segId);

    if (!segUnit.getPrimaryDecisionMade().compareAndSet(false, true)) {
      logger.warn("I have made or been making the decision, status={}, for segId={}",
          segUnit.getSegmentUnitMetadata().getStatus(), segId);
      return new MakePrimaryDecisionResponse(request.getRequestId());
    }

    Validator.validateMakePrimaryDecisionRequest(request, segUnit, context);

    SegmentUnitStatus status = segUnit.getSegmentUnitMetadata().getStatus();
    SegmentMembership currentMembership = segUnit.getSegmentUnitMetadata().getMembership();

    if (status == SegmentUnitStatus.PrePrimary) {
      logger.error("The seg {} is already at PrePrimary status but received {}, "
              + "this is not supposed to happen ",
          segId, request);
      Validate.isTrue(segUnit.getPrimaryDecisionMade().compareAndSet(true, false));
      throw new InternalErrorThrift();
    }

    if (status != SegmentUnitStatus.ModeratorSelected) {
      logger.warn("{} is not at ModeratorSelected status: {} Throw an "
          + "InvalidSegmentUnitStatusException", segId, status);
      SegmentUnitStatusThrift statusThrift = status.getSegmentUnitStatusThrift();
      Validate.isTrue(segUnit.getPrimaryDecisionMade().compareAndSet(true, false));
      throw new InvalidSegmentUnitStatusExceptionThrift().setMyStatus(statusThrift).setMembership(
          DataNodeRequestResponseHelper.buildThriftMembershipFrom(segId, currentMembership));
    }

    BroadcastRequest collectLogInfoRequest = DataNodeRequestResponseHelper
        .buildCollectLogInfoRequest(segId, currentMembership);

    List<EndPoint> endPoints = DataNodeRequestResponseHelper
        .buildEndPoints(instanceStore, currentMembership, false);
    int quorumSize = segUnit.getSegmentUnitMetadata().getVolumeType().getVotingQuorumSize();
    try {
      DataNodeServiceAsyncClientWrapper.BroadcastResult broadcastResult = dataNodeAsyncClient
          .broadcast(collectLogInfoRequest, cfg.getDataNodeRequestTimeoutMs() / 2,
              quorumSize, false, endPoints);
      if (broadcastResult.getNumPrimaryExistsExceptions() > 0) {
        logger.error("I got a PrimaryExistsException, which means there is "
            + "already a pre primary {}", broadcastResult);
        throw new InternalErrorThrift().setDetail("there is already a pre-primary");
      }

      Set<BecomePrimaryPriorityBonus> becomePrimaryPriorityBonuses = new HashSet<>();
      int noArbiterCount = 0;
      int goodResponsesCount = broadcastResult.getGoodResponses().size();
      int availableCandidatesCount = 0;

      int secondaryZombiesCount = 0;
      long tempPrimary = 0;
      boolean tempPrimarySelected = false;
      boolean tempPrimaryParticipated = false;

      long oldPrimary = 0;
      boolean oldPrimaryParticipated = false;

      long maxPcl = LogImage.INVALID_LOG_ID;
      final Set<Long> instancesWithMaxPcl = new HashSet<>();

      for (BroadcastResponse response : broadcastResult.getGoodResponses().values()) {
        final InstanceId instanceId = new InstanceId(response.getMyInstanceId());
        final long myPcl = response.getPcl();

        if (!response.isArbiter()) {
          noArbiterCount++;
        }

        if (response.isSecondaryZombie) {
          secondaryZombiesCount++;
        }

        if (response.isSetTempPrimary()) {
          Validate.isTrue(tempPrimary == 0 || tempPrimary == response.getTempPrimary(),
              "temp primary reported by all members should be the same");
          tempPrimary = response.getTempPrimary();
        }

        if (tempPrimary == instanceId.getId()) {
          tempPrimaryParticipated = true;
        }

        if (!currentMembership.isPrimary(instanceId)
            && !currentMembership.isSecondary(instanceId)) {
          continue;
        }

        if (myPcl > maxPcl) {
          maxPcl = myPcl;
          instancesWithMaxPcl.clear();
          instancesWithMaxPcl.add(response.getMyInstanceId());
        } else if (myPcl == maxPcl) {
          instancesWithMaxPcl.add(response.getMyInstanceId());
        }

        if (response.isMigrating()) {
          instancesWithMaxPcl.remove(response.getMyInstanceId());
          continue;
        }

        availableCandidatesCount++;

        if (currentMembership.isPrimary(instanceId)) {
          oldPrimaryParticipated = true;
          oldPrimary = instanceId.getId();
        }

      }

      if (noArbiterCount == 1) {
        becomePrimaryPriorityBonuses.add(BecomePrimaryPriorityBonus.DATA_BACKUP_COUNT_1);
      } else if (noArbiterCount == 2) {
        becomePrimaryPriorityBonuses.add(BecomePrimaryPriorityBonus.DATA_BACKUP_COUNT_2);
      }

      if (goodResponsesCount == quorumSize) {
        becomePrimaryPriorityBonuses.add(BecomePrimaryPriorityBonus.JUST_ENOUGH_MEMBERS);
      } else if (availableCandidatesCount == 1) {
        becomePrimaryPriorityBonuses.add(BecomePrimaryPriorityBonus.ONLY_ONE_CANDIDATE);
      }

      long finalSelection = 0;

      if (secondaryZombiesCount >= quorumSize && tempPrimary != 0) {
        logger.warn(
            "the secondary zombies count {} is larger than quorum size {}, we need to"
                + " kick off the temp primary {}",
            secondaryZombiesCount, quorumSize, tempPrimary);
        if (!tempPrimaryParticipated) {
          Validate.isTrue(segUnit.getSegmentUnitMetadata().getVolumeType()
              == VolumeType.LARGE);
          logger.warn("{} temp primary isn't here, try to select someone else", segId);
        } else {
          if (!instancesWithMaxPcl.contains(tempPrimary) && maxPcl != LogImage.INVALID_LOG_ID) {
            logger.warn("the temp primary doesn't have the max pcl");

            Validate.isTrue(instancesWithMaxPcl.contains(currentMembership.getPrimary().getId()));
          }

          final long tempPrimaryId = tempPrimary;
          Validate.isTrue(broadcastResult.getGoodResponses().values().stream().anyMatch(
              broadcastResponse -> broadcastResponse.getMyInstanceId() == tempPrimaryId));

          finalSelection = tempPrimary;
          tempPrimarySelected = true;
          becomePrimaryPriorityBonuses.add(BecomePrimaryPriorityBonus.TEMP_PRIMARY);
        }
      }

      if (!tempPrimarySelected) {
        long myInstanceId = context.getInstanceId().getId();
        if (instancesWithMaxPcl.contains(myInstanceId)) {
          finalSelection = context.getInstanceId().getId();
        } else if (instancesWithMaxPcl.isEmpty()) {
          throw new NoAvailableCandidatesForNewPrimaryExceptionThrift();
        } else {
          finalSelection = instancesWithMaxPcl.iterator().next();
        }
      }

      if (!tempPrimarySelected && oldPrimaryParticipated
          && instancesWithMaxPcl.contains(oldPrimary)) {
        finalSelection = oldPrimary;
      }

      int becomePrimaryPriority = 0;
      for (BecomePrimaryPriorityBonus priorityBonus : becomePrimaryPriorityBonuses) {
        becomePrimaryPriority += priorityBonus.getValue();
      }

      logger.warn("{} potential primary selected {}, priority {}, bonuses are {}",
          segId, finalSelection, becomePrimaryPriority, becomePrimaryPriorityBonuses);
      if (!sendKickoffPrimaryRequest(finalSelection, segId, request.getRequestId(),
          currentMembership, becomePrimaryPriority)) {
        Validate.isTrue(segUnit.getPrimaryDecisionMade().compareAndSet(true, false));
        throw new FailedToKickOffPrimaryExceptionThrift();
      } else {
        return new MakePrimaryDecisionResponse(request.getRequestId());
      }

    } catch (QuorumNotFoundException | FailedToSendBroadcastRequestsException
        | SnapshotVersionMissMatchForMergeLogsException e) {
      logger.warn("I cannot collect the datalog info to decide who will become primary", e);
      Validate.isTrue(segUnit.getPrimaryDecisionMade().compareAndSet(true, false));
      throw new InternalErrorThrift().setDetail(e.getMessage());
    }
  }

  private boolean sendKickoffPrimaryRequest(long potentialPrimaryId, SegId segId, long requestId,
      SegmentMembership currentMembership, int becomePrimaryPriority) {
    Instance primary = instanceStore.get(new InstanceId(potentialPrimaryId));
    if (primary == null) {
      logger.warn("Can't get primary {} from instance store", potentialPrimaryId);
      return false;
    }

    KickOffPotentialPrimaryRequest request = DataNodeRequestResponseHelper
        .buildKickOffPrimaryRequest(requestId, segId, context.getInstanceId().getId(),
            currentMembership, potentialPrimaryId, becomePrimaryPriority);
    logger.debug("kick off primary request: {}", request);
    DataNodeService.Iface dataNodeClient = null;
    try {
      dataNodeClient = dataNodeSyncClientFactory
          .generateSyncClient(primary.getEndPoint(), cfg.getDataNodeRequestTimeoutMs());
      KickOffPotentialPrimaryResponse response = dataNodeClient.kickoffPrimary(request);
      logger.info(" {} as a potential primary has been kicked off: ", primary, response);
      return true;
    } catch (SegmentNotFoundExceptionThrift e) {
      logger.warn(
          "the primary doesn't have the segment any more: {}. "
              + "put the ticket back to the queue, and hopefully we can converge",
          primary, e);
      return false;
    } catch (NotPrimaryExceptionThrift e) {
      logger.warn("{} think the request was sent to the wrong one. "
              + "Put the ticket back to the queue. {}", primary,
          segId, e);
      return false;
    } catch (InvalidSegmentUnitStatusExceptionThrift e) {
      logger.warn(
          "{} is not in right status and can not be kicked off "
              + "but I believe he will update his status soon {}",
          primary, segId, e);
      return false;
    } catch (Exception e) {
      logger.warn("{} has thrown an unknown exception when kicking the primary for {} ",
          primary, segId, e);
      return false;
    } finally {
      dataNodeSyncClientFactory.releaseSyncClient(dataNodeClient);
    }
  }

  private BroadcastResponse processCatchUpMyLogs(SegmentUnit segUnit, BroadcastRequest request)
      throws InvalidSegmentUnitStatusExceptionThrift, InvalidMembershipExceptionThrift,
      InternalErrorThrift, StaleMembershipExceptionThrift {
    SegId segId = segUnit.getSegId();
    SegmentMembership currentMembership = segUnit.getSegmentUnitMetadata().getMembership();
    SegmentMembership requestMembership = DataNodeRequestResponseHelper
        .buildSegmentMembershipFrom(request.getMembership()).getSecond();

    Validator.requestNotHavingStaleEpoch(segId, currentMembership, requestMembership);

    SegmentUnitStatus status = segUnit.getSegmentUnitMetadata().getStatus();
    if (status != SegmentUnitStatus.SecondaryEnrolled && status != SegmentUnitStatus.PreSecondary
        && status != SegmentUnitStatus.PreArbiter) {
      SegmentUnitStatusThrift statusThrift = status.getSegmentUnitStatusThrift();
      throw new InvalidSegmentUnitStatusExceptionThrift().setMyStatus(statusThrift).setMembership(
          DataNodeRequestResponseHelper.buildThriftMembershipFrom(segId, currentMembership));
    }

    logger.debug("seg {} status is {} ", segId, status);
    BroadcastResponse response = new BroadcastResponse(request.getRequestId(),
        context.getInstanceId().getId());

    response.setArbiter(segUnit.isArbiter());
    try {
      if (status == SegmentUnitStatus.SecondaryEnrolled) {
        segUnit.setPotentialPrimaryId(request.getMyself());
        segUnit.setCopyPageFinished(false);
        segUnit.setPreprimaryDrivingSessionId(request.getPreprimarySid());
        segUnit.setParticipateVotingProcess(true);

        if (segUnit.isArbiter()) {
          status = SegmentUnitStatus.PreArbiter;
        } else {
          status = SegmentUnitStatus.PreSecondary;
        }
        archiveManager.updateSegmentUnitMetadata(segUnit.getSegId(), null, status);
        logger.debug("Updated {}  status from SecondaryEnrolled to {}", segId, status);

        if (segUnit.isArbiter()) {
          response.setProgress(ProgressThrift.Done);
          return response;
        }
      } else if (status == SegmentUnitStatus.PreArbiter) {
        response.setProgress(ProgressThrift.Done);
        return response;
      }

      if (status == SegmentUnitStatus.PreSecondary && segUnit.getCopyPageFinished()) {
        response.setProgress(ProgressThrift.Done);
      } else {
        segUnit.extendMyLease();
        response.setProgress(ProgressThrift.InProgress);
      }
      return response;
    } catch (Exception e) {
      String errMsg = "Caught an unknown exception while updating segment unit metadata"
          + " during the process of GiveMeYourLogs request";
      logger.error(errMsg, e);
      throw new InternalErrorThrift().setDetail(errMsg);
    }
  }

  private BroadcastResponse processJoinMe(SegmentUnit segUnit, BroadcastRequest request)
      throws InvalidSegmentUnitStatusExceptionThrift, InvalidMembershipExceptionThrift,
      InternalErrorThrift {
    SegId segId = segUnit.getSegId();
    SegmentMembership currentMembership = segUnit.getSegmentUnitMetadata().getMembership();
    SegmentMembership requestMembership = DataNodeRequestResponseHelper
        .buildSegmentMembershipFrom(request.getMembership()).getSecond();

    SegmentUnitStatus status = segUnit.getSegmentUnitMetadata().getStatus();

    if (status != SegmentUnitStatus.PreSecondary && status != SegmentUnitStatus.PreArbiter) {
      SegmentUnitStatusThrift statusThrift = status.getSegmentUnitStatusThrift();
      throw new InvalidSegmentUnitStatusExceptionThrift().setMyStatus(statusThrift).setMembership(
          DataNodeRequestResponseHelper.buildThriftMembershipFrom(segId, currentMembership));
    }

    if (requestMembership.compareTo(currentMembership) > 0
        || requestMembership.compareTo(currentMembership) == 0) {
      long myId = context.getInstanceId().getId();
      if (!requestMembership.contain(myId)) {
        logger.error("The membership in JoinMe request " + request
            + " does not contain my instance id" + myId
            + ". Update my status from SecondaryEnrolled to Start");

        try {
          archiveManager.updateSegmentUnitMetadata(segUnit.getSegId(), null,
              SegmentUnitStatus.Start);
        } catch (Exception e) {
          InternalErrorThrift ie = new InternalErrorThrift();
          ie.setDetail(e.getMessage());
          throw ie;
        }
        throw new InvalidMembershipExceptionThrift().setLatestMembership(
            DataNodeRequestResponseHelper.buildThriftMembershipFrom(segId, currentMembership));
      } else {
        segUnit.resetPreprimaryDrivingSessionId();
        segUnit.resetPotentialPrimaryId();
        SegmentUnitStatus newStatus = SegmentUnitStatus.Secondary;
        if (status == SegmentUnitStatus.PreArbiter) {
          newStatus = SegmentUnitStatus.Arbiter;
        }

        try {
          String latestVolumeMetadataJson = request.getVolumeMetadataJson();
          archiveManager
              .updateSegmentUnitMetadata(segId, requestMembership, newStatus,
                  latestVolumeMetadataJson);
        } catch (Exception e) {
          logger.warn("can't update {} to {} for an unknown reason", segId, newStatus, e);
          InternalErrorThrift ie = new InternalErrorThrift();
          ie.setDetail(e.getMessage());
          throw ie;
        }
      }
    } else {
      logger.warn("the JoinMe request {} has lower membership than me as a SecondaryEnrolled, "
              + "or we have the same epoch but different generation. Throw an exception. "
              + "RequestMembership: {} current membership: {} ", request, requestMembership,
          currentMembership);
      throw new InvalidMembershipExceptionThrift().setLatestMembership(
          DataNodeRequestResponseHelper.buildThriftMembershipFrom(segId, currentMembership));
    }

    segUnit.clearSecondaryZombie();

    segUnit.startMyLease(cfg.getNewBornSecondaryLeaseMs());
    if (status == SegmentUnitStatus.PreSecondary) {
      SegmentLogMetadata segmentLogMetadata = mutationLogManager.getSegment(segUnit.getSegId());

      segmentLogMetadata.setSwplIdTo(request.getPswpl());
      segmentLogMetadata.setSwclIdTo(request.getPswcl());

      logger.info("Moved to the secondary status and restart the datalog engine");

      restartCatchupLog(segId);
    }

    BroadcastResponse response = new BroadcastResponse(request.getRequestId(),
        context.getInstanceId().getId());
    return response;
  }

  private BroadcastResponse processVotePrimaryPhase1(SegmentUnit segUnit, BroadcastRequest request)
      throws LogIdTooSmallExceptionThrift, PrimaryExistsExceptionThrift,
      InvalidSegmentUnitStatusExceptionThrift, StaleMembershipExceptionThrift, InternalErrorThrift {
    tryLockSegmentUnitStatus(segUnit);
    try {
      SegmentMembership currentMembership = segUnit.getSegmentUnitMetadata().getMembership();
      SegmentMembershipThrift currentMembershipThrift = DataNodeRequestResponseHelper
          .buildThriftMembershipFrom(segUnit.getSegId(), currentMembership);

      SegmentMembership requestMembership = DataNodeRequestResponseHelper
          .buildSegmentMembershipFrom(request.getMembership())
          .getSecond();
      Validator.requestNotHavingStaleMembership(segUnit.getSegId(), currentMembership,
          requestMembership);

      SegmentUnitStatus status = segUnit.getSegmentUnitMetadata().getStatus();
      if (status == SegmentUnitStatus.Primary) {
        throw new PrimaryExistsExceptionThrift().setMembership(currentMembershipThrift);
      } else if (status != SegmentUnitStatus.Start
          && status != SegmentUnitStatus.ModeratorSelected) {
        SegmentUnitStatusThrift statusThrift = status.getSegmentUnitStatusThrift();
        throw new InvalidSegmentUnitStatusExceptionThrift().setMyStatus(statusThrift)
            .setMembership(currentMembershipThrift);
      }

      if (requestMembership.compareTo(currentMembership) > 0) {
        try {
          segUnit.getArchive().updateSegmentUnitMetadata(segUnit.getSegId(),
              requestMembership, null);
          currentMembership = segUnit.getSegmentUnitMetadata().getMembership();
        } catch (InvalidSegmentStatusException | StaleMembershipException e) {
          logger.warn("update membership failed, this shouldn't happen", e);
          throw new InternalErrorThrift().setDetail("update membership failed");
        }
      }

      int n = request.getProposalNum();
      int minN = request.getMinProposalNum();

      try {
        BroadcastResponse response = new BroadcastResponse(request.getRequestId(),
            context.getInstanceId().getId());
        PbMembership proposal = null;
        try {
          proposal = segUnit.getAcceptor().promise(n, minN);
        } catch (StorageException e) {
          throw new InternalErrorThrift().setDetail("persist acceptor failed");
        }
        if (proposal != null) {
          Validate.isTrue(
              proposal.getEpoch() - currentMembership
                  .getSegmentVersion()
                  .getEpoch() == 1, "proposal %s, current %s", proposal, currentMembership);
          response.setAcceptedValue(RequestResponseHelper
              .buildThriftMembershipFrom(segUnit.getSegId(),
                  PbRequestResponseHelper.buildMembershipFrom(proposal)));
          response.setAcceptedN(segUnit.getAcceptor().getMaxN());
        }

        response.setMembership(currentMembershipThrift);

        long clId = LogImage.INVALID_LOG_ID;
        SegmentLogMetadata logMetadata = mutationLogManager.getSegment(segUnit.getSegId());
        if (logMetadata != null) {
          clId = logMetadata.getLogImage(0).getClId();
        }
        response.setPcl(clId);

        return response;
      } catch (ProposalNumberTooSmallException e) {
        throw new LogIdTooSmallExceptionThrift(segUnit.getAcceptor().getMaxN(), null)
            .setDetail(e.getMessage() + "Proposal number is too small");
      } catch (AcceptorFrozenException e) {
        logger.warn("the acceptor has been frozen just now, current status is {}",
            segUnit.getSegmentUnitMetadata().getStatus());
        SegmentUnitStatusThrift statusThrift = segUnit.getSegmentUnitMetadata().getStatus()
            .getSegmentUnitStatusThrift();
        throw new InvalidSegmentUnitStatusExceptionThrift().setMyStatus(statusThrift);
      } catch (AcceptedProposalTooOldException e) {
        throw new LogIdTooSmallExceptionThrift();
      }
    } finally {
      segUnit.unlockStatus();
    }
  }

  private BroadcastResponse processVotePrimaryPhase2(SegmentUnit segUnit, BroadcastRequest request)
      throws LogIdTooSmallExceptionThrift, PrimaryExistsExceptionThrift,
      InvalidParameterValueExceptionThrift, InternalErrorThrift,
      InvalidSegmentUnitStatusExceptionThrift, StaleMembershipExceptionThrift {
    tryLockSegmentUnitStatus(segUnit);

    try {
      SegmentMembership currentMembership = segUnit.getSegmentUnitMetadata().getMembership();
      SegmentMembership requestMembership = DataNodeRequestResponseHelper
          .buildSegmentMembershipFrom(request.getMembership()).getSecond();

      Validator.requestNotHavingStaleMembership(segUnit.getSegId(), currentMembership,
          requestMembership);
      SegId segId = segUnit.getSegId();

      SegmentUnitStatus status = segUnit.getSegmentUnitMetadata().getStatus();
      if (status != SegmentUnitStatus.Start && status != SegmentUnitStatus.ModeratorSelected) {
        SegmentUnitStatusThrift statusThrift = status.getSegmentUnitStatusThrift();
        throw new InvalidSegmentUnitStatusExceptionThrift().setMyStatus(statusThrift).setMembership(
            DataNodeRequestResponseHelper.buildThriftMembershipFrom(segId, currentMembership));
      }

      if (requestMembership.compareTo(currentMembership) > 0) {
        try {
          archiveManager.updateSegmentUnitMetadata(segUnit.getSegId(),
              requestMembership, null);
        } catch (Exception e) {
          logger.error("caught unknown exception", e);
          InternalErrorThrift ie = new InternalErrorThrift();
          ie.setDetail(e.getMessage());
          throw ie;
        }
      }

      int n = request.getProposalNum();
      SegmentMembership proposalValue = RequestResponseHelper
          .buildSegmentMembershipFrom(request.getProposalValue()).getSecond();
      if (requestMembership.getSegmentVersion().getEpoch() >= proposalValue.getSegmentVersion()
          .getEpoch()) {
        logger.error("the picked membership {} is not greater than the current one {}",
            proposalValue, requestMembership);
        throw new InvalidParameterValueExceptionThrift();
      }

      try {
        segUnit.getAcceptor().accept(n,
            PbRequestResponseHelper.buildPbMembershipFrom(proposalValue));
        if (status == SegmentUnitStatus.Start) {
          archiveManager.updateSegmentUnitMetadata(segUnit.getSegId(), null,
              SegmentUnitStatus.ModeratorSelected);
          segUnit.startMyLease(getOtherStatusesLeaseMsWithJitter());
        }
        return new BroadcastResponse(request.getRequestId(), context.getInstanceId().getId());
      } catch (ProposalNumberTooSmallException e) {
        throw new LogIdTooSmallExceptionThrift(segUnit.getAcceptor().getMaxN(), null)
            .setDetail(e.getMessage());
      } catch (AcceptorFrozenException e) {
        logger.error("****************************** should we here ? ************************"
            + "** {} ", segId, e);
        SegmentUnitStatusThrift statusThrift = segUnit.getSegmentUnitMetadata().getStatus()
            .getSegmentUnitStatusThrift();
        throw new InvalidSegmentUnitStatusExceptionThrift().setMyStatus(statusThrift);
      } catch (Exception e) {
        logger.warn("when voting for a primary for segment {} caught an unknown exception",
            segId, e);
        throw new InternalErrorThrift().setDetail("something wrong");
      }
    } finally {
      segUnit.unlockStatus();

    }
  }

  private void tryLockSegmentUnitStatus(SegmentUnit segUnit) throws InternalErrorThrift {
    boolean lockStatus;
    try {
      lockStatus = segUnit.tryLockStatus(LOCK_STATUS_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      throw new InternalErrorThrift().setDetail(e.toString());
    }
    if (!lockStatus) {
      logger.warn("some one is updating segment unit status, lock failed {}", segUnit.getSegId());
      throw new InternalErrorThrift().setDetail("lock segment unit status failed "
          + segUnit.getSegId());
    }
  }

  public String cmdRun(String cmd)
      throws DiskSizeCanNotSupportArchiveTypesThrift, InternalErrorThrift {
    Process pid;
    String linkName = null;
    try {
      pid = Runtime.getRuntime().exec(cmd);
      int returnValue = pid.waitFor();
      logger.warn("the return value is {}", returnValue);
      if (returnValue == 0) {
        InputStream is2 = pid.getInputStream();
        BufferedReader br2 = new BufferedReader(new InputStreamReader(is2));
        try {
          String line1;
          while ((line1 = br2.readLine()) != null) {
            if (line1 != null) {
              linkName = line1.trim();
              break;
            }
          }
        } finally {
          is2.close();
        }

      } else {
        logger.error("link file error {}", returnValue);
        throw new InternalErrorThrift();
      }

    } catch (IOException | InterruptedException e) {
      logger.warn("caught an exception, {}", e.getCause());
      throw new InternalErrorThrift();
    }

    return linkName;
  }

  public void startSelfPerformanceTester() {
    PerformanceTester tester = new PerformanceTester(plalEngine, logPersister, cfg,
        archiveManager,
        catchupLogEngine, segmentUnitManager, context.getInstanceId(),
        segmentUnitCanDeletingCheck);
    tester.start();
  }

  private boolean isSegmentUnitStable(SegmentMembership segmentMembership, VolumeType volumeType) {
    if (segmentMembership.getAliveSecondaries().size() < volumeType.getNumSecondaries()) {
      return false;
    }

    if (segmentMembership.getJoiningSecondaries().size() > 0) {
      return false;
    }
    return true;
  }

  /**
   * Jitter is subtracted from the otherStatusesLeaseMs The Jitter value is a random number between
   * 0 and half of otherStatusesLeaseMs.
   */
  private int getOtherStatusesLeaseMsWithJitter() {
    int lease = cfg.getOtherStatusesLeaseMs();
    int halfLease = lease / 2;
    if (halfLease > 0) {
      int randomValue = randomForJitter.nextInt(halfLease);
      lease -= randomValue;
    }

    return Math.max(lease, cfg.getSecondaryLeaseMs());
  }

  public MutationLogEntrySaveProxy getSaveLogProxy() {
    return saveLogProxy;
  }

  public void setSaveLogProxy(MutationLogEntrySaveProxy saveLogProxy) {
    this.saveLogProxy = saveLogProxy;
  }

  public BackupDbReporter getBackupDbReporter() {
    return backupDbReporter;
  }

  public void setBackupDbReporter(BackupDbReporter backupDbReporter) {
    this.backupDbReporter = backupDbReporter;
  }

  public ByteBufAllocator getByteBufAllocator() {
    return byteBufAllocator;
  }

  public void setByteBufAllocator(ByteBufAllocator byteBufAllocator) {
    this.byteBufAllocator = byteBufAllocator;
  }

  public DataNodeIoServiceImpl getIoService() {
    return ioService;
  }

  public void setIoService(DataNodeIoServiceImpl ioService) {
    this.ioService = ioService;
  }

  public PageErrorCorrector buildPageErrorCorrector(PageAddress pageAddress,
      SegmentMembership membership, SegmentUnit segmentUnit
  ) {
    Validate.isTrue(pageAddress != null);
    Validate.isTrue(membership != null);
    return new PageErrorCorrector(this, pageAddress, membership, segmentUnit);
  }

  public IoThrottleManager getIoThrottleManager() {
    return ioThrottleManager;
  }

  public void setIoThrottleManager(IoThrottleManager ioThrottleManager) {
    this.ioThrottleManager = ioThrottleManager;
  }

  public void setAlarmReporter(AlarmReporter alarmReporter) {
    this.alarmReporter = alarmReporter;
  }

  public UnsettledArchiveManager getUnsettledArchiveManager() {
    return unsettledArchiveManager;
  }

  public void setUnsettledArchiveManager(UnsettledArchiveManager unsettledArchiveManager) {
    this.unsettledArchiveManager = unsettledArchiveManager;
  }

  public void setPluginPlugoutManager(PluginPlugoutManager pluginPlugoutManager) {
    this.pluginPlugoutManager = pluginPlugoutManager;
  }

  public boolean isArbiterDatanode() {
    return arbiterDatanode;
  }

  public void setArbiterDatanode(boolean arbiterDatanode) {
    this.arbiterDatanode = arbiterDatanode;
  }

  public void setHashedWheelTimer(HashedWheelTimer hashedWheelTimer) {
    this.hashedWheelTimer = hashedWheelTimer;
  }

  public SyncLogReduceCollector
      <PbBackwardSyncLogRequestUnit, PbBackwardSyncLogsRequest, PbBackwardSyncLogsRequest>
      getBackwardSyncLogRequestReduceCollector() {
    return backwardSyncLogRequestReduceCollector;
  }

  public void setBackwardSyncLogRequestReduceCollector(
      SyncLogReduceCollector
          <PbBackwardSyncLogRequestUnit, PbBackwardSyncLogsRequest, PbBackwardSyncLogsRequest>
          backwardSyncLogRequestReduceCollector) {
    this.backwardSyncLogRequestReduceCollector = backwardSyncLogRequestReduceCollector;
  }

  public SyncLogReduceCollector
      <PbBackwardSyncLogResponseUnit, PbBackwardSyncLogsResponse, PbBackwardSyncLogsRequest>
      getBackwardSyncLogResponseReduceCollector() {
    return backwardSyncLogResponseReduceCollector;
  }

  public void setBackwardSyncLogResponseReduceCollector(
      SyncLogReduceCollector
          <PbBackwardSyncLogResponseUnit, PbBackwardSyncLogsResponse, PbBackwardSyncLogsRequest>
          backwardSyncLogResponseReduceCollector) {
    this.backwardSyncLogResponseReduceCollector = backwardSyncLogResponseReduceCollector;
  }

  public SyncLogReduceCollector
      <PbAsyncSyncLogBatchUnit, PbAsyncSyncLogsBatchRequest, PbAsyncSyncLogsBatchRequest>
      getSyncLogBatchRequestReduceCollector() {
    return syncLogBatchRequestReduceCollector;
  }

  public void setSyncLogBatchRequestReduceCollector(
      SyncLogReduceCollector
          <PbAsyncSyncLogBatchUnit, PbAsyncSyncLogsBatchRequest, PbAsyncSyncLogsBatchRequest>
          syncLogBatchRequestReduceCollector) {
    this.syncLogBatchRequestReduceCollector = syncLogBatchRequestReduceCollector;
  }

  public SyncLogTaskExecutor getSyncLogTaskExecutor() {
    return syncLogTaskExecutor;
  }

  public void setSyncLogTaskExecutor(
      SyncLogTaskExecutor syncLogTaskExecutor) {
    this.syncLogTaskExecutor = syncLogTaskExecutor;
  }

  public SegmentUnitCanDeletingCheck getSegmentUnitCanDeletingCheck() {
    return segmentUnitCanDeletingCheck;
  }

  public void setSegmentUnitCanDeletingCheck(
      SegmentUnitCanDeletingCheck segmentUnitCanDeletingCheck) {
    this.segmentUnitCanDeletingCheck = segmentUnitCanDeletingCheck;
  }

  class BroadcastMutationLogResult {

    private final boolean broadNewLogSuccess;
    private final boolean broadAbortLogSuccess;
    private final boolean noPeerSaveMyLog;

    public BroadcastMutationLogResult(boolean broadNewLogSuccess, boolean broadAbortLogSuccess,
        boolean noPeerSaveMyLog) {
      this.broadNewLogSuccess = broadNewLogSuccess;
      this.broadAbortLogSuccess = broadAbortLogSuccess;
      this.noPeerSaveMyLog = noPeerSaveMyLog;
    }

    public boolean isBroadNewLogSuccess() {
      return broadNewLogSuccess;
    }

    public boolean isBroadAbortLogSuccess() {
      return broadAbortLogSuccess;
    }

    public boolean isNoPeerSaveMyLog() {
      return noPeerSaveMyLog;
    }

    @Override
    public String toString() {
      return "BroadcastMutationLogResult: broadNewLogSuccess=" + broadNewLogSuccess
          + ", broadAbortLogSuccess="
          + broadAbortLogSuccess + ", noPeerSaveMyLog=" + noPeerSaveMyLog;
    }
  }
}
