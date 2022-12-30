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

import com.google.common.collect.ImmutableSet;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.RequestResponseHelper;
import py.archive.ArchiveOptions;
import py.archive.page.PageAddress;
import py.archive.segment.MigrationStatus;
import py.archive.segment.SegId;
import py.archive.segment.SegmentLeaseHandler;
import py.archive.segment.SegmentUnitBitmap;
import py.archive.segment.SegmentUnitMetadata;
import py.archive.segment.SegmentUnitStatus;
import py.archive.segment.SegmentUnitType;
import py.archive.segment.recurring.MigratingSegmentUnitMetadata;
import py.common.RequestIdBuilder;
import py.common.struct.EndPoint;
import py.common.struct.Pair;
import py.datanode.archive.RawArchive;
import py.datanode.archive.ShadowPageManager;
import py.datanode.checksecondaryinactive.CheckSecondaryInactive;
import py.datanode.configuration.DataNodeConfiguration;
import py.datanode.page.impl.PageAddressGenerator;
import py.datanode.segment.copy.PrimaryCopyPageManager;
import py.datanode.segment.copy.SecondaryCopyPageManager;
import py.datanode.segment.datalog.LogImage;
import py.datanode.segment.datalog.SegmentLogMetadata;
import py.datanode.segment.datalog.persist.LogStorageReader;
import py.datanode.segment.datalog.plal.engine.PlalEngine;
import py.datanode.segment.datalog.sync.log.driver.SecondaryPclDriver;
import py.datanode.segment.datalogbak.catchup.PclDrivingType;
import py.datanode.segment.datalogbak.catchup.PlalDriver;
import py.datanode.segment.membership.Lease;
import py.datanode.segment.membership.statemachine.checksecondaryinactive.NewSegmentUnitExpirationThreshold;
import py.datanode.segment.membership.statemachine.processors.MigrationUtils;
import py.datanode.segment.membership.vote.AcceptorIface;
import py.datanode.segment.membership.vote.AcceptorPersister;
import py.datanode.segment.membership.vote.AcceptorPersisterWithRawStorage;
import py.datanode.segment.membership.vote.DefaultAcceptorPersister;
import py.datanode.segment.membership.vote.PersistedAcceptor;
import py.exception.LeaseExtensionFrozenException;
import py.exception.StorageException;
import py.infocenter.client.InformationCenterClientFactory;
import py.instance.InstanceId;
import py.membership.SegmentForm;
import py.membership.SegmentMembership;
import py.proto.Broadcastlog.PbMembership;
import py.storage.Storage;
import py.thrift.datanode.service.HeartbeatResultThrift;
import py.thrift.datanode.service.JoiningGroupRequestDeniedExceptionThrift;
import py.thrift.infocenter.service.CreateSegmentUnitRequest;
import py.thrift.infocenter.service.InformationCenter;
import py.thrift.share.InternalErrorThrift;
import py.thrift.share.SegmentUnitTypeThrift;
import py.volume.VolumeId;
import py.volume.VolumeType;

public class SegmentUnit implements SegmentLeaseHandler, ShadowPageManager {
  public static final long NO_POTENTIAL_PRIMARY_ID = 0;
  public static final long EMPTY_PREPRIMARY_DRIVING_SESSION_ID = 0;
  private static final Logger logger = LoggerFactory.getLogger(SegmentUnit.class);
  private static final int MIN_DEFAULT_FINALIZE_COUNT = 1000;
  private static final int drivePLALLeaseMS = 60 * 1000;
  private final SegmentUnitMetadata metadata;
  // for inner migrating
  private final Object innerMigratingLock = new Object();
  // For synchronizing between creating segment unit and voting.
  private final AtomicBoolean segmentUnitCreateDone;
  private final boolean firstUnitInVolume;
  // end for inner migrating
  private final long createTime = System.currentTimeMillis();
  private final AtomicBoolean primaryDecisionMade = new AtomicBoolean(false);
  // just for set primary cl
  private final AtomicBoolean isPrimaryClSetting = new AtomicBoolean(false);
  private final ReentrantLock firstWriteLock = new ReentrantLock();
  private final ReentrantLock statusLock = new ReentrantLock();
  private final ReentrantLock peerDepartingLock = new ReentrantLock();
  private final Set<Integer> firstWritePageIndex = Collections
      .newSetFromMap(new ConcurrentHashMap<>());
  // the archive is not final anymore because a segment unit might be migrated to another archive
  private volatile RawArchive archive;
  private boolean innerMigrating;
  private MigratingSegmentUnitMetadata metadataOnMigratingDestination;
  private RawArchive archiveOfMigratingDestination;
  private AcceptorIface<Integer, PbMembership> acceptor;
  // Lease is only set by proposer
  private Lease myLease;
  private Map<InstanceId, Lease> peerLeases;
  private Map<InstanceId, Pair<Long, BitSet>> expiredPeersClAndMissingPages;
  private Map<InstanceId, HeartbeatResultThrift> heartbeatResult;

  private PlalEngine plalEngine;
  private long whenNewSegmentUnitWasRequested;
  private boolean stateContextsHaveBeenRemoved;
  private boolean logContextsHaveBeenRemoved;
  private AtomicLong primaryBeingKicked = new AtomicLong(NO_POTENTIAL_PRIMARY_ID);
  private AtomicBoolean havingKickedOffProcessOfBecomingPrimary = new AtomicBoolean(false);
  private volatile boolean disallowPrePrimaryPclDriving;

  private volatile boolean disallowZombieSecondaryPclDriving;

  private AtomicBoolean copyPageFinished = new AtomicBoolean(false);

  private AtomicBoolean participateVotingProcess = new AtomicBoolean(false);
  // keep a reference to SegmentLogMetadata so as to clear up all stuff
  // related to swcl and swpl when the segment state is changed to start
  private volatile SegmentLogMetadata segmentLogMetadata;
  // When this flag is set, the segment unit's lease can't be extended
  // This flag is used by a secondary to refuse to extends
  private boolean holdToExtendLease;
  // mark the service is shutdown
  private boolean dataNodeServiceShutdown;
  // for debug only, disable extend peer lease
  private boolean disableExtendPeerLease = false;
  private boolean disableProcessWriteMutationLog = false;
  // for temporily hold on voting when a secondary is created at the first time
  private volatile boolean isJustCreated = true;

  //for copy page
  private volatile SecondaryCopyPageManager secondaryCopyPageManager;
  private volatile PrimaryCopyPageManager primaryCopyPageManager;
  // for continuous servicing
  private volatile long pclWhenBecomeSecondaryZombie = LogImage.INVALID_LOG_ID;
  private volatile boolean secondaryZombie = false;
  // in pssaa, the tp may die and a new tp will be selected,
  // so only a zombie flag is not enough, we need a zombie again flag to distinct this
  private volatile boolean secondaryZombieAgain = false;
  private AtomicBoolean zombieConfirmed = new AtomicBoolean();
  private AtomicBoolean zombieConfirmedAgain = new AtomicBoolean();
  private InstanceId potentialTempPrimary = null;
  private long preprimaryDrivingSessionId = EMPTY_PREPRIMARY_DRIVING_SESSION_ID;
  // when network is unhealthy, pause segment unit's voting process
  private volatile boolean pauseVotingProcess = false;
  // for test purpose
  private boolean allowReadDataEvenNotPrimary = false;
  private SecondaryPclDriver secondaryPclDriver;
  private AtomicBoolean isProcessingPclDriverForSecondary = new AtomicBoolean(false);
  private Lease drivePlalLease = new Lease();
  //    private final SegmentUnitAddressManager addressManager;
  private SegmentUnitMultiAddressManager multiAddressManager;

  private volatile ImmutableSet<EndPoint> availableSecondary = ImmutableSet.of();
  private AtomicBoolean isPausedCatchUpLogForSecondary = new AtomicBoolean(false);
  private AtomicLong lastMinRecoverLogId = new AtomicLong(0);

  public SegmentUnit(SegmentUnitMetadata metadata, RawArchive archive) {
    this(metadata, archive, defaultAcceptorPersister(metadata, archive));
  }

  public SegmentUnit(SegmentUnitMetadata metadata, RawArchive archive,
      AcceptorPersister<Integer, PbMembership> acceptorPersister) {
    this.metadata = metadata;
    this.archive = archive;

    try {
      this.acceptor = new PersistedAcceptor<>(acceptorPersister);

      PbMembership uncommittedMembership = acceptor.getAcceptedProposal();
      if (uncommittedMembership != null && uncommittedMembership.getEpoch() <= metadata
          .getMembership().getSegmentVersion()
          .getEpoch()) {
        acceptor.init();
        if (metadata.getStatus() == SegmentUnitStatus.Start) {
          acceptor.open();
        }
      }
    } catch (StorageException e) {
      logger.warn("fail to restore acceptor", e);
      throw new IllegalArgumentException(e);
    }

    this.myLease = new Lease();
    this.peerLeases = new HashMap<>();
    this.expiredPeersClAndMissingPages = new ConcurrentHashMap<>();
    this.heartbeatResult = new HashMap<>();
    this.whenNewSegmentUnitWasRequested = System.currentTimeMillis();
    this.firstUnitInVolume = (metadata.getSegId().getIndex() == 0);
    this.stateContextsHaveBeenRemoved = false;
    this.logContextsHaveBeenRemoved = metadata.isArbiter() ? true : false;
    this.segmentUnitCreateDone = new AtomicBoolean(true);
    this.holdToExtendLease = false;
    this.multiAddressManager = new SegmentUnitMultiAddressManagerImpl(this);

    drivePlalLease.extend(drivePLALLeaseMS);
  }

  private static AcceptorPersister<Integer, PbMembership> defaultAcceptorPersister(
      SegmentUnitMetadata metadata, RawArchive archive) {
    if (archive == null || archive.getStorage() == null) {
      return new DefaultAcceptorPersister();
    } else {
      return new AcceptorPersisterWithRawStorage(metadata.getSegId(), archive.getStorage(),
          metadata.getMetadataOffsetInArchive() + ArchiveOptions.SEGMENTUNIT_METADATA_LENGTH
              + ArchiveOptions.SEGMENTUNIT_BITMAP_LENGTH,
          ArchiveOptions.SEGMENTUNIT_ACCEPTOR_LENGTH, ArchiveOptions.SECTOR_SIZE, false);
    }

  }

  public final boolean tryLockStatus(long timeout, TimeUnit timeUnit)
      throws InterruptedException {
    return statusLock.tryLock(timeout, timeUnit);
  }

  public final void lockStatus() {
    statusLock.lock();
  }

  public final void unlockStatus() {
    statusLock.unlock();
  }

  public SecondaryPclDriver getSecondaryPclDriver() {
    return secondaryPclDriver;
  }

  public void setSecondaryPclDriver(
      SecondaryPclDriver secondaryPclDriver) {
    this.secondaryPclDriver = secondaryPclDriver;
  }

  public boolean isSegmentUnitCreateDone() {
    return segmentUnitCreateDone.get();
  }

  public void setSegmentUnitCreateDone(boolean createDone) {
    segmentUnitCreateDone.set(createDone);
  }

  public void setWhenNewSegmentUnitWasRequested(long whenNewSegmentUnitWasRequested) {
    this.whenNewSegmentUnitWasRequested = whenNewSegmentUnitWasRequested;
  }

  public SegId getSegId() {
    return metadata.getSegId();
  }

  public RawArchive getArchive() {
    return archive;
  }

  public SegmentUnitMetadata getSegmentUnitMetadata() {
    return metadata;
  }

  public VolumeId getVolumeId() {
    return getSegId().getVolumeId();
  }

  public long getStartLogicalOffset() {
    return metadata.getLogicalDataOffset();
  }

  public long getStartPhysicalOffset() {
    return metadata.getPhysicalDataOffset();
  }

  public long getEndLogicalOffset() {
    return getStartLogicalOffset() + ArchiveOptions.SEGMENT_PHYSICAL_SIZE;
  }

  public long getEndPhysicalOffset() {
    return getStartPhysicalOffset() + ArchiveOptions.SEGMENT_PHYSICAL_SIZE;
  }

  public void saveHeartbeatResult(InstanceId instance,
      HeartbeatResultThrift heartbeatResultFromSecondary) {
    synchronized (heartbeatResult) {
      this.heartbeatResult.put(instance, heartbeatResultFromSecondary);
    }
  }

  public Map<InstanceId, HeartbeatResultThrift> getHearbeatResult() {
    Map<InstanceId, HeartbeatResultThrift> map = new HashMap<InstanceId, HeartbeatResultThrift>();
    synchronized (heartbeatResult) {
      for (Entry<InstanceId, HeartbeatResultThrift> entry : heartbeatResult.entrySet()) {
        map.put(entry.getKey(), entry.getValue());
      }
    }
    return map;
  }

  public void clearHeartbeatResult() {
    synchronized (heartbeatResult) {
      this.heartbeatResult.clear();
    }
  }

  public void removeHeartbeatResult(InstanceId instance) {
    synchronized (heartbeatResult) {
      this.heartbeatResult.remove(instance);
    }
  }

  @Override
  public boolean extendMyLease() throws LeaseExtensionFrozenException {
    if (holdToExtendLease) {
      throw new LeaseExtensionFrozenException();
    }
    logger.trace("extending my lease {} when segId {} is at {}", myLease,
        getSegmentUnitMetadata().getSegId(),
        getSegmentUnitMetadata().getStatus());
    return myLease.extend();
  }

  @Override
  public boolean extendMyLease(long lease) throws LeaseExtensionFrozenException {
    if (holdToExtendLease) {
      throw new LeaseExtensionFrozenException();
    }
    logger.trace("extending my lease  {} when segId {} is at {} with the new value {}", myLease,
        getSegmentUnitMetadata().getSegId(), getSegmentUnitMetadata().getStatus(), lease);
    return myLease.extend(lease);
  }

  @Override
  public void startMyLease(long lease) {
    logger.trace("starting my lease  {} when segId {} is at {} with the new span {}", myLease,
        getSegmentUnitMetadata().getSegId(), getSegmentUnitMetadata().getStatus(), lease);
    myLease.start(lease);
    holdToExtendLease = false;
  }

  @Override
  public void clearMyLease() {
    logger.trace("clearing my lease {} when segId {} is at {}", myLease,
        getSegmentUnitMetadata().getSegId(),
        getSegmentUnitMetadata().getStatus());
    myLease.clear();
    holdToExtendLease = false;
  }

  @Override
  public boolean myLeaseExpired() {
    logger.trace("checking my lease {} when segId {} is at {}", myLease,
        getSegmentUnitMetadata().getSegId(),
        getSegmentUnitMetadata().getStatus());
    return myLease.expire();
  }

  @Override
  public boolean myLeaseExpired(SegmentUnitStatus status) {
    if (status.equals(SegmentUnitStatus.Primary)) {
      return primaryLeaseExpired().getFirst();
    } else {
      boolean leaseExpired = myLeaseExpired();
      if (leaseExpired) {
        logger
            .warn(" {}  is at {} status and its lease has expired, lease is {}", getSegId(), status,
                myLease);
      }
      return leaseExpired;
    }
  }

  @Override
  public Pair<Boolean, Set<InstanceId>> primaryLeaseExpired() {
    int numOfAliveSecondaries = 0;
    boolean peerExpired = false;
    Map<InstanceId, Lease> expiredPeers = new HashMap<InstanceId, Lease>();
    Pair<Boolean, Set<InstanceId>> returnPair = new Pair<Boolean, Set<InstanceId>>();
    synchronized (peerLeases) {
      for (Map.Entry<InstanceId, Lease> entry : peerLeases.entrySet()) {
        if (!entry.getValue().expire()) {
          numOfAliveSecondaries++;
        } else {
          peerExpired = true;
          expiredPeers.put(entry.getKey(), entry.getValue());
        }
      }
    }

    if (peerExpired) {
      logger.debug("some instances lease expired {} at the primary {}", expiredPeers, getSegId());
    }

    returnPair.setSecond(expiredPeers.keySet());

    int quorumSize = this.metadata.getVolumeType().getVotingQuorumSize();

    if (numOfAliveSecondaries >= quorumSize - 1) {
      returnPair.setFirst(false);
    } else {
      logger.warn("{} majority of secondaries leases at the primary expired. "
              + "The primary lease is considered expired."
              + " My current status is {}, number of alive {}, quorum {}, number of peers {}",
          getSegId(), metadata.getStatus(), numOfAliveSecondaries, quorumSize, peerLeases.size());

      returnPair.setFirst(true);
    }
    return returnPair;
  }

  @Override
  public void startPeerLeases(long leaseSpan, Collection<InstanceId> peers) {
    logger.trace("start the peer lease: {}  is at {}. The lease span is span {}",
        getSegmentUnitMetadata().getSegId(), getSegmentUnitMetadata().getStatus(), leaseSpan);
    synchronized (peerLeases) {
      peerLeases.clear();
      for (InstanceId peer : peers) {
        Lease lease = new Lease();
        peerLeases.put(peer, lease);
        lease.start(leaseSpan);
      }
    }
  }

  @Override
  public boolean extendPeerLease(long leaseSpan, InstanceId peer) {
    Lease lease = null;
    boolean success = true;

    if (disableExtendPeerLease) {
      return success;
    }

    synchronized (peerLeases) {
      lease = peerLeases.get(peer);
      if (lease != null) {
        success = lease.extend();
        if (!success) {
          if (!primaryLeaseExpired().getFirst()) {
            logger.trace(" the primary still holds the lease. Let's extend the lease by force");
            success = lease.extend(true);
          }
        }
      } else {
        success = false;
      }
    }

    if (!success) {
      logger.info("extend peer {} lease failed, current lease {}", peer, lease);
    }

    return success;
  }

  @Override
  public void startPeerLease(long leaseSpan, InstanceId peer) {
    Lease lease = null;
    synchronized (peerLeases) {
      peerLeases.remove(peer);
      lease = new Lease();
      peerLeases.put(peer, lease);
      lease.start(leaseSpan);
    }
    logger
        .debug("started peer lease:{} {} is at {} lease: {} ", getSegmentUnitMetadata().getSegId(),
            peer,
            getSegmentUnitMetadata().getStatus(), lease);
  }

  @Override
  public void clearPeerLeases() {
    logger.trace("clear peer lease: {} is at {} ", getSegmentUnitMetadata().getSegId(),
        getSegmentUnitMetadata().getStatus());
    synchronized (peerLeases) {
      peerLeases.clear();
    }
  }

  public AcceptorIface<Integer, PbMembership> getAcceptor() {
    return acceptor;
  }

  public void requestNewSegmentUnit(InformationCenterClientFactory informationCenterClientFactory,
      long newSegmentUnitRequestExpirationThresholdInMs,
      CheckSecondaryInactive checkSecondaryInactive) {
    synchronized (this) {
      SegmentMembership membership = metadata.getMembership();
      VolumeType volumeType = metadata.getVolumeType();
      SegId segId = getSegId();
      if (needToRequestNewSegmentUnit(membership, checkSecondaryInactive,
          newSegmentUnitRequestExpirationThresholdInMs)) {
        logger
            .warn("Requesting a new segment unit for segId={}, membership={}", metadata.getSegId(),
                membership);

        SegmentUnitTypeThrift segmentUnitTypeThrift = figureOutWhichTypeToBeCreated(membership,
            volumeType);
        try {
          InformationCenter.Iface infocenterClient = informationCenterClientFactory.build()
              .getClient();
          CreateSegmentUnitRequest request = new CreateSegmentUnitRequest(RequestIdBuilder.get(),
              segId.getVolumeId().getId(), segId.getIndex(), volumeType.getVolumeTypeThrift(),
              segmentUnitTypeThrift, System.currentTimeMillis(),
              newSegmentUnitRequestExpirationThresholdInMs,
              metadata.isEnableLaunchMultiDrivers(),
              metadata.getVolumeSource().getVolumeSourceThrift())
              .setInitMembership(
                  RequestResponseHelper.buildThriftMembershipFrom(segId, membership));
          infocenterClient.createSegmentUnit(request);
        } catch (Exception e) {
          logger.debug(
              "Failed to send a createNewSegmentUnit request to control center, membership: {}",
              membership, e);
        }
      }
    }
  }

  private boolean secondaryHaveAllLogWhenHaveIs(SegmentMembership currentMembership) {
    boolean secondaryHaveAllLog = true;
    if (getSegmentUnitMetadata().getVolumeType() == VolumeType.REGULAR) {
      Validate
          .isTrue(currentMembership.getAliveSecondariesWithoutArbitersAndCandidate().size() == 1);
      InstanceId aliveSecondary = currentMembership.getAliveSecondariesWithoutArbitersAndCandidate()
          .iterator().next();
      if (segmentLogMetadata.getPrimaryMaxLogIdWhenPsi() > segmentLogMetadata
          .getPeerClId(aliveSecondary)) {
        secondaryHaveAllLog = false;
      }
    }
    return secondaryHaveAllLog;
  }

  public SegmentMembership addSecondary(InstanceId newMember, boolean isArbiter, int peerLeaseSpan,
      boolean isSecondaryCandidate)
      throws JoiningGroupRequestDeniedExceptionThrift, InternalErrorThrift {
    logger.info("a new member want to join us {} {} {} {}", newMember, isArbiter,
        isSecondaryCandidate,
        peerLeaseSpan);
    SegmentMembership currentMembership = new SegmentMembership(metadata.getMembership());
    SegmentMembership newMembership = null;
    StringBuilder errStringBuilder = null;
    VolumeType volumeType = metadata.getVolumeType();

    if (currentMembership.contain(newMember)) {
      if (currentMembership.isArbiter(newMember)) {
        Validate.isTrue(isArbiter);
        newMembership = currentMembership;
      } else if (currentMembership.isSecondary(newMember) || currentMembership
          .isJoiningSecondary(newMember)) {
        Validate.isTrue(!isArbiter);
        newMembership = currentMembership;
      } else if (currentMembership.isInactiveSecondary(newMember)) {
        if (isArbiter) {
          if (currentMembership.getArbiters().size() == volumeType.getNumArbiters()) {
            logger.warn("{} we already have enough arbiters but {} came, current membership is {}",
                getSegId(), newMember, currentMembership);
            newMembership = null;
          } else {
            newMembership = currentMembership.inactiveSecondaryBecomeArbiter(newMember);
          }
        } else {
          if (currentMembership.getAliveSecondariesWithoutArbitersAndCandidate().size() == metadata
              .getVolumeType().getNumSecondaries()) {
            logger
                .warn("{} we already have enough secondaries but {} came, current membership is {}",
                    getSegId(), newMember, currentMembership);
            newMembership = null;
          } else {
            boolean alreadyHasJoiningSecondary =
                currentMembership.getJoiningSecondaries().size() != 0;
            if (alreadyHasJoiningSecondary) {
              newMembership = null;
              errStringBuilder = new StringBuilder("There is no slot for the new member ")
                  .append(newMember).append(" and the current membership ")
                  .append(currentMembership);
            } else {
              newMembership = currentMembership.inactiveSecondaryBecomeJoining(newMember);
            }
          }
        }
      } else if (currentMembership.isSecondaryCandidate(newMember)) {
        newMembership = isSecondaryCandidate ? currentMembership : null;
      } else {
        logger
            .error("{} the request member {} is in a strange position in membership {}", getSegId(),
                newMember, currentMembership);
        throw new IllegalArgumentException(
            "the request member is in a strange position in membership");
      }
    } else {
      boolean addNewMember =
          currentMembership.getAllSecondaries().size() < volumeType.getNumMembers() - 1;
      boolean alreadyHasJoiningSecondary = currentMembership.getJoiningSecondaries().size() != 0;
      boolean havingSlotForNewMember = isArbiter

          ? (currentMembership.getArbiters().size() < volumeType.getNumArbiters()) :

          (!alreadyHasJoiningSecondary && (
              currentMembership.getAliveSecondariesWithoutArbitersAndCandidate().size() < volumeType
                  .getNumSecondaries()));
      if (!havingSlotForNewMember) {
        if (isSecondaryCandidate && !alreadyHasJoiningSecondary
            && currentMembership.getSecondaryCandidate() == null && currentMembership
            .getInactiveSecondaries().isEmpty()) {
          Validate.isTrue(!isArbiter);
          newMembership = currentMembership.addSecondaryCandidate(newMember);
        } else {
          newMembership = null;
          errStringBuilder = new StringBuilder("There is no slot for the new member ")
              .append(newMember)
              .append(" and the current membership ").append(currentMembership);
        }
      } else if (addNewMember) {
        newMembership = isArbiter
            ? currentMembership.addArbiters(newMember) :
            currentMembership.addJoiningSecondary(newMember);
      } else if (!secondaryHaveAllLogWhenHaveIs(currentMembership)) {
        newMembership = null;
        errStringBuilder = new StringBuilder("There is secondary have down. "
            + "However there is no S have all Log By PSS.").append(newMember)
            .append(" in the membership").append(currentMembership);
      } else {
        synchronized (peerLeases) {
          InstanceId mostInactiveSecondary = searchMostInactiveSecondary(currentMembership,
              NewSegmentUnitExpirationThreshold.getBogusCheckSecondary());
          if (mostInactiveSecondary != null) {
            newMembership = isArbiter
                ? currentMembership
                .removeInactiveSecondaryAndAddArbiter(mostInactiveSecondary, newMember) :
                currentMembership
                    .removeInactiveSecondaryAndAddJoiningSecondary(mostInactiveSecondary,
                        newMember);
            if (!currentMembership.equals(metadata.getMembership())) {
              logger.warn("{} membership has been changed just now {} {}", getSegId(),
                  currentMembership,
                  metadata.getMembership());
              newMembership = null;
              errStringBuilder = new StringBuilder("Membership has been changed just now");
            } else {
              peerLeases.remove(mostInactiveSecondary);
              startPeerLease(peerLeaseSpan, newMember);
            }
          } else {
            newMembership = null;
            errStringBuilder = new StringBuilder("A new segment unit is being added. "
                + "However there is no secondary which is expired.").append(newMember)
                .append(" in the membership").append(currentMembership);
          }
        }
      }
    }

    if (!currentMembership.equals(metadata.getMembership())) {
      logger.warn("{} membership has been changed just now {} {}", getSegId(), currentMembership,
          metadata.getMembership());
      newMembership = null;
      errStringBuilder = new StringBuilder("Membership has been changed just now");
    }

    if (newMembership != null) {
      logger.debug("after adding {} to {}, the new membership is {}", newMember, currentMembership,
          newMembership);

      whenNewSegmentUnitWasRequested = 0L;
      startPeerLease(peerLeaseSpan, newMember);
      return newMembership;
    } else {
      if (errStringBuilder == null) {
        errStringBuilder = new StringBuilder("membership is ").append(currentMembership.toString())
            .append("addmember ").append(newMember).append("is arbiter ").append(isArbiter);
      }
      logger.info(errStringBuilder.toString());

      if (MigrationUtils.explicitlyDenyNewSecondary(metadata.getMembership(), newMember, isArbiter,
          volumeType)) {
        logger.warn("denying a secondary from joining, "
                + "this may cause the secondary to delete itself!!, {} {} {} {}",
            metadata, newMember, volumeType, isArbiter);
        JoiningGroupRequestDeniedExceptionThrift e = new JoiningGroupRequestDeniedExceptionThrift();
        e.setDetail(errStringBuilder.toString());
        throw e;
      } else {
        throw new InternalErrorThrift().setDetail(errStringBuilder.toString());
      }
    }
  }

  public boolean isDisallowPrePrimaryPclDriving() {
    return disallowPrePrimaryPclDriving;
  }

  public void setDisallowPrePrimaryPclDriving(boolean disallowPrePrimaryPclDriving) {
    this.disallowPrePrimaryPclDriving = disallowPrePrimaryPclDriving;
  }

  public boolean isDisallowZombieSecondaryPclDriving() {
    return disallowZombieSecondaryPclDriving;
  }

  public void setDisallowZombieSecondaryPclDriving(boolean disallowZombieSecondaryPclDriving) {
    this.disallowZombieSecondaryPclDriving = disallowZombieSecondaryPclDriving;
  }

  public boolean havingKickedOffProcessOfBecomingPrimary(long potentialPrimaryId) {
    if (!havingKickedOffProcessOfBecomingPrimary.compareAndSet(false, true)) {
      logger.warn("the BecomePrimary thread has been kicking off for segUnit {}. do nothing ",
          metadata.getSegId());
      return true;
    }

    logger.warn(
        "the BecomePrimary thread has NOT YET been kicking off"
            + " for segUnit {}, the potentialPrimaryId: "
            + "{} primary being kicked {}", metadata.getSegId(), potentialPrimaryId,
        primaryBeingKicked);
    SegmentUnitStatus status = metadata.getStatus();
    if (status != SegmentUnitStatus.PrePrimary) {
      logger.warn(
          "But a BecomePrimary thread has previously changed {} "
              + "status from PrePrimary to {}. Do nothing",
          metadata.getSegId(), status);
      havingKickedOffProcessOfBecomingPrimary.set(false);
      return true;
    } else {
      return false;
    }
  }

  public void resetProcessOfBecomingPrimaryBeenKickedoff() {
    havingKickedOffProcessOfBecomingPrimary.set(false);
  }

  public boolean isBecomingPrimary() {
    return havingKickedOffProcessOfBecomingPrimary.get();
  }

  public void resetPotentialPrimaryId() {
    setPotentialPrimaryId(NO_POTENTIAL_PRIMARY_ID);
  }

  public long getPotentialPrimaryId() {
    return primaryBeingKicked.get();
  }

  public void setPotentialPrimaryId(long potentialPrimaryId) {
    primaryBeingKicked.set(potentialPrimaryId);
  }

  public boolean potentialPrimaryIdSet() {
    return primaryBeingKicked.get() != NO_POTENTIAL_PRIMARY_ID;
  }

  public boolean getCopyPageFinished() {
    return copyPageFinished.get();
  }

  public void setCopyPageFinished(boolean copyPageFinished) {
    if (!this.copyPageFinished.compareAndSet(!copyPageFinished, copyPageFinished)) {
      logger.info(
          "fail to set the value copyPageFinished to {}, for segId={}, due to its value is {}",
          copyPageFinished, getSegId(), this.copyPageFinished.get());
    }
  }

  private InstanceId searchMostInactiveSecondary(SegmentMembership currentMembership,
      CheckSecondaryInactive secondaryInactive) {
    InstanceId mostInactiveSecondary = null;
    long earliestExpirationTime = Long.MAX_VALUE;
    for (InstanceId secondary : currentMembership.getInactiveSecondaries()) {
      Lease lease = peerLeases.get(secondary);
      logger.debug("lease is {}, secondary is {}", lease, secondary);

      if (lease != null && lease.expire()) {
        long expirationTime = lease.getExpirationTime();
        if (expirationTime < earliestExpirationTime) {
          earliestExpirationTime = expirationTime;
          mostInactiveSecondary = secondary;
        }
      }
    }

    if (mostInactiveSecondary != null) {
      logger.debug("found a secondary {} whose lease has expired", mostInactiveSecondary);

      boolean expiredForLongEnough;
      boolean missingEnoughLogs;
      boolean missingEnoughPages;

      long howLongItHasBeenExpired = System.currentTimeMillis() - earliestExpirationTime;
      expiredForLongEnough = secondaryInactive
          .waitTimeout(TimeUnit.MILLISECONDS.toSeconds(howLongItHasBeenExpired));

      boolean reallyInactive = false;
      if (expiredForLongEnough) {
        logger.debug("the expiration time ({}) is long enough to exceed the threshold: {}",
            howLongItHasBeenExpired);
        reallyInactive = true;
      } else {
        logger.debug("the expiration time ({}) is not long enough to exceed the threshold: {}",
            howLongItHasBeenExpired);
      }

      Pair<Long, BitSet> clAndMissingPages = expiredPeersClAndMissingPages
          .get(mostInactiveSecondary);
      if (clAndMissingPages != null) {
        long howFarBehindItsClIs = getSegmentLogMetadata().getClId() - clAndMissingPages.getFirst();
        missingEnoughLogs = secondaryInactive.missTooManyLogs(howFarBehindItsClIs);

        int howManyMissingPages = clAndMissingPages.getSecond().cardinality();
        missingEnoughPages = secondaryInactive.missTooManyPages(howManyMissingPages);

        if (missingEnoughLogs) {
          logger.debug("the missing logs ({}) is large enough to exceed the threshold: {}",
              howFarBehindItsClIs);
          reallyInactive = true;
        } else {
          logger.debug("the missing logs ({}) is not large enough to exceed the threshold: {}",
              howFarBehindItsClIs);
        }

        if (missingEnoughPages) {
          logger.debug("the missing pages ({}) is large enough to exceed the threshold: {}",
              howManyMissingPages);
          reallyInactive = true;
        } else {
          logger.debug("the missing pages ({}) is not large enough to exceed the threshold: {}",
              howManyMissingPages);
        }
      } else {
        logger.info("the cl and missing pages of an expired member {} is null {}",
            mostInactiveSecondary,
            getSegId());
      }

      if (!reallyInactive) {
        mostInactiveSecondary = null;
      } else {
        expiredPeersClAndMissingPages.remove(mostInactiveSecondary);
      }
    }

    return mostInactiveSecondary;
  }

  private boolean segmentHasLessUnits(SegmentMembership membership,
      CheckSecondaryInactive checkSecondaryInactive) {
    InstanceId expiredSecondary = searchMostInactiveSecondary(membership, checkSecondaryInactive);

    return expiredSecondary == null ? false : true;
  }

  private SegmentUnitTypeThrift figureOutWhichTypeToBeCreated(SegmentMembership membership,
      VolumeType volumeType) {
    SegmentUnitTypeThrift segmentUnitTypeThrift = null;
    if (membership.getArbiters().size() < volumeType.getNumArbiters()) {
      segmentUnitTypeThrift = SegmentUnitTypeThrift.Arbiter;
    } else {
      segmentUnitTypeThrift = metadata.getSegmentUnitType().getSegmentUnitTypeThrift();
    }
    return segmentUnitTypeThrift;
  }

  private boolean needToRequestNewSegmentUnit(SegmentMembership membership,
      CheckSecondaryInactive checkSecondaryInactive,
      long newSegmentUnitRequestExpirationThresholdInMs) {
    long span = System.currentTimeMillis() - whenNewSegmentUnitWasRequested;
    if (segmentHasLessUnits(membership, checkSecondaryInactive)
        && span > newSegmentUnitRequestExpirationThresholdInMs) {
      logger.warn(
          "segment {} has less member {} and it has been long time {} "
              + "ms since last requests for a new segment.",
          getSegId(), membership, span);
      if (getSegmentUnitMetadata().getVolumeType() != VolumeType.REGULAR) {
        return true;
      }
      for (InstanceId instanceId : membership.getAliveSecondariesWithoutArbitersAndCandidate()) {
        long clId = segmentLogMetadata.getPeerClId(instanceId);
        if (segmentLogMetadata.getPrimaryMaxLogIdWhenPsi() <= clId) {
          return true;
        } else {
          logger.warn("primary logID is {}, secondary pcl id {}",
              segmentLogMetadata.getPrimaryMaxLogIdWhenPsi(), clId);
        }
      }
    }
    return false;
  }

  @Override
  public String toString() {
    return "SegmentUnit [metadata=" + metadata + "]";
  }

  public boolean isFirstUnitInVolume() {
    return firstUnitInVolume;
  }

  public SegmentLogMetadata getSegmentLogMetadata() {
    return segmentLogMetadata;
  }

  public void setSegmentLogMetadata(SegmentLogMetadata segmentLogMetadata) {
    this.segmentLogMetadata = segmentLogMetadata;
    this.segmentLogMetadata.setSegmentUnit(this);
  }

  public void setParticipateVotingProcess(boolean b) {
    participateVotingProcess.set(b);
  }

  public boolean hasParticipatedVotingProcess() {
    return participateVotingProcess.get();
  }

  public Lease getMyLease() {
    return myLease;
  }

  public void setStateContextsHaveBeenRemoved() {
    stateContextsHaveBeenRemoved = true;
  }

  public void setLogContextsHaveBeenRemoved() {
    logContextsHaveBeenRemoved = true;
  }

  public boolean isStateContextsHaveBeenRemoved() {
    return stateContextsHaveBeenRemoved;
  }

  public boolean isLogContextsHaveBeenRemoved() {
    return logContextsHaveBeenRemoved;
  }

  public boolean isHoldToExtendLease() {
    return holdToExtendLease;
  }

  public void setHoldToExtendLease(boolean holdToExtendLease) {
    this.holdToExtendLease = holdToExtendLease;
  }

  public boolean isDisableExtendPeerLease() {
    return disableExtendPeerLease;
  }

  public void setDisableExtendPeerLease(boolean disableExtendPeerLease) {
    this.disableExtendPeerLease = disableExtendPeerLease;
  }

  public boolean isDisableProcessWriteMutationLog() {
    return disableProcessWriteMutationLog;
  }

  public void setDisableProcessWriteMutationLog(boolean disableProcessWriteMutationLog) {
    this.disableProcessWriteMutationLog = disableProcessWriteMutationLog;
  }

  public boolean preprimaryDrivingSessionIdMatched(long otherPreprimaryDrivingSessionId) {
    return preprimaryDrivingSessionId == otherPreprimaryDrivingSessionId;
  }

  public long getPreprimaryDrivingSessionId() {
    return preprimaryDrivingSessionId;
  }

  public void setPreprimaryDrivingSessionId(long preprimaryDrivingSessionId) {
    this.preprimaryDrivingSessionId = preprimaryDrivingSessionId;
  }

  public void resetPreprimaryDrivingSessionId() {
    this.preprimaryDrivingSessionId = EMPTY_PREPRIMARY_DRIVING_SESSION_ID;
  }

  public boolean isDataNodeServiceShutdown() {
    return dataNodeServiceShutdown;
  }

  public void setDataNodeServiceShutdown(boolean dataNodeServiceShutdown) {
    this.dataNodeServiceShutdown = dataNodeServiceShutdown;
  }

  public boolean isNotAcceptWriteReadRequest() {
    return this.dataNodeServiceShutdown
        || this.getSegmentUnitMetadata().getStatus() == SegmentUnitStatus.OFFLINING;
  }

  public boolean isJustCreated(long timeoutMs) {
    if (isJustCreated) {
      if (System.currentTimeMillis() - createTime > timeoutMs) {
        isJustCreated = false;
      }
    }

    return isJustCreated;
  }

  public PclDrivingType getPclDrivingType(InstanceId myself) {
    SegmentUnitStatus currentStatus = metadata.getStatus();
    SegmentMembership membership = metadata.getMembership();
    if (currentStatus == SegmentUnitStatus.PrePrimary && !disallowPrePrimaryPclDriving) {
      return PclDrivingType.Primary;
    }

    if (membership.isPrimary(myself)) {
      if (currentStatus == SegmentUnitStatus.Primary) {
        return PclDrivingType.Primary;
      }

      if (isNotAcceptWriteReadRequest()) {
        return PclDrivingType.OfflinePrimary;
      }

      SegmentForm segmentForm = SegmentForm.getSegmentForm(membership, metadata.getVolumeType());

      if (membership.isQuorumUpdated() && !segmentForm.canGenerateNewPrimary()) {
        return PclDrivingType.OrphanPrimary;
      }

      if (currentStatus == SegmentUnitStatus.PreSecondary) {
        if (potentialPrimaryIdSet()) {
          return PclDrivingType.VotingSecondary;
        }
      }
      return PclDrivingType.NotDriving;
    } else {
      if (currentStatus == SegmentUnitStatus.Secondary) {
        return PclDrivingType.Secondary;
      }

      if (currentStatus == SegmentUnitStatus.PreSecondary) {
        if (hasParticipatedVotingProcess()) {
          return PclDrivingType.VotingSecondary;
        } else {
          return PclDrivingType.JoiningSecondary;
        }
      }

      if (currentStatus == SegmentUnitStatus.OFFLINING) {
        return PclDrivingType.OfflineSecondary;
      }
    }

    return PclDrivingType.NotDriving;
  }

  public PageAddress generatePageAddress(long logicOffsetInSegUnit) {
    return PageAddressGenerator
        .generate(this.getSegId(), getStartLogicalOffset(), logicOffsetInSegUnit,
            getArchive().getStorage(),
            ArchiveOptions.PAGE_SIZE);
  }

  public PageAddress generatePhysicalPageAddress(long logicOffsetInSegUnit) {
    Validate.isTrue(getSegmentUnitMetadata().getSegmentUnitType().equals(SegmentUnitType.Normal));
    return PageAddressGenerator
        .generate(this.getSegId(), getStartPhysicalOffset(), logicOffsetInSegUnit,
            getArchive().getStorage(),
            ArchiveOptions.PAGE_SIZE);
  }

  public long getPageIndexerOffset() {
    return metadata.getPageIndexerOffset();
  }

  public PlalEngine getPlalEngine() {
    return plalEngine;
  }

  public void setPlalEngine(PlalEngine plalEngine) {
    this.plalEngine = plalEngine;
  }

  public boolean isAllowReadDataEvenNotPrimary() {
    return allowReadDataEvenNotPrimary;
  }

  public void setAllowReadDataEvenNotPrimary(boolean allowReadDataEvenNotPrimary) {
    this.allowReadDataEvenNotPrimary = allowReadDataEvenNotPrimary;
  }

  public boolean isSecondaryZombie() {
    return secondaryZombie;
  }

  public boolean isSecondaryZombieAgain() {
    return secondaryZombieAgain;
  }

  public void markSecondaryZombie() {
    if (!secondaryZombie) {
      synchronized (zombieConfirmed) {
        if (!secondaryZombie) {
          setHoldToExtendLease(true);
          this.secondaryZombie = true;
          this.disallowZombieSecondaryPclDriving = true;
        }
      }
    }
  }

  public void markSecondaryZombieAgain() {
    this.secondaryZombieAgain = true;
  }

  public Object getLockForZombiePcl() {
    return zombieConfirmed;
  }

  public int confirmSecondaryZombie(boolean again, InstanceId tempPrimary) {
    int incGeneration = metadata.getMembership().getAllSecondaries().size();
    if (!again) {
      Validate.isTrue(isSecondaryZombie());
      if (zombieConfirmed.compareAndSet(false, true)) {
        potentialTempPrimary = tempPrimary;
        return incGeneration;
      } else {
        if (!tempPrimary.equals(potentialTempPrimary)) {
          logger.warn("new temp primary confirmed to replace old one {} {}", tempPrimary,
              potentialTempPrimary);
          potentialTempPrimary = tempPrimary;
          return incGeneration * 2;
        } else {
          if (metadata.getMembership().getTempPrimary() == null) {
            return incGeneration;
          } else {
            return 0;
          }
        }
      }
    } else {
      Validate.isTrue(isSecondaryZombieAgain());
      if (zombieConfirmedAgain.compareAndSet(false, true)) {
        return incGeneration;
      } else {
        return 0;
      }
    }
  }

  public long getPclWhenBecomeSecondaryZombie() {
    Validate.isTrue(isSecondaryZombie());
    return pclWhenBecomeSecondaryZombie;
  }

  public void setPclWhenBecomeSecondaryZombie(long pcl) {
    pclWhenBecomeSecondaryZombie = pcl;
  }

  public void clearSecondaryZombie() {
    setHoldToExtendLease(false);
    setDisallowZombieSecondaryPclDriving(false);
    this.secondaryZombie = false;
    this.secondaryZombieAgain = false;
    this.zombieConfirmed.set(false);
    this.zombieConfirmedAgain.set(false);
    this.potentialTempPrimary = null;
    this.pclWhenBecomeSecondaryZombie = LogImage.INVALID_LOG_ID;
  }

  public boolean isArbiter() {
    return metadata.isArbiter();
  }

  public SecondaryCopyPageManager getSecondaryCopyPageManager() {
    return secondaryCopyPageManager;
  }

  public void setSecondaryCopyPageManager(SecondaryCopyPageManager secondaryCopyPageManager) {
    this.secondaryCopyPageManager = secondaryCopyPageManager;
  }

  public PrimaryCopyPageManager getPrimaryCopyPageManager() {
    return primaryCopyPageManager;
  }

  public void setPrimaryCopyPageManager(PrimaryCopyPageManager primaryCopyPageManager) {
    this.primaryCopyPageManager = primaryCopyPageManager;
  }

  public void forgetAboutBeingOneSecondaryCandidate() {
    if (metadata.isSecondaryCandidate()) {
      logger.warn("{} forget about being a secondary candidate", getSegId());
      metadata.setSecondaryCandidate(false);
      metadata.setReplacee(null);
      try {
        archive.persistSegmentUnitMetaAndBitmap(metadata);
      } catch (Exception e) {
        logger.warn("can't persist not being a secondary candidate", e);
      }
    }
  }

  public boolean isOriginPyhiscalAddress(long pyhiscalAddress) {
    if (getSegmentUnitMetadata().getSegmentUnitType().equals(SegmentUnitType.Normal)) {
      if (getSegmentUnitMetadata().getBrickMetadata() != null) {
        return pyhiscalAddress >= getStartLogicalOffset()
            && pyhiscalAddress < getEndPhysicalOffset();
      }
    }

    return false;
  }

  public AtomicBoolean getPrimaryDecisionMade() {
    return primaryDecisionMade;
  }

  public boolean tryLockPeerDeparting() {
    return peerDepartingLock.tryLock();
  }

  public void unlockPeerDeparting() {
    peerDepartingLock.unlock();
  }

  public void memberExpired(InstanceId peer, boolean isArbiter) {
    lockStatus();
    try {
      if (metadata.getStatus() != SegmentUnitStatus.Primary) {
        return;
      }
      segmentLogMetadata.removePeerPlId(peer);
      long peerCl = segmentLogMetadata.removePeerClId(peer);
      if (!isArbiter && peerCl != LogImage.IMPOSSIBLE_LOG_ID) {
        int pageCount = (int) (ArchiveOptions.SEGMENT_SIZE / ArchiveOptions.PAGE_SIZE);
        Validate.isTrue(
            null == expiredPeersClAndMissingPages
                .put(peer, new Pair<>(peerCl, new BitSet(pageCount))));
      }
    } finally {
      unlockStatus();
    }
  }

  public void clearExpiredMembers() {
    expiredPeersClAndMissingPages.clear();
  }

  public void recordFailedMigration(SegId segId) {
    archive.addMigrateFailedSegId(segId);
  }

  public boolean setPageHasBeenWritten(int pageIndex) {
    boolean needSet = false;
    if (metadata.isPageFree(pageIndex)) {
      metadata.setPage(pageIndex);
      metadata.setBitmapNeedPersisted(true);
      needSet = true;
    }

    for (Pair<Long, BitSet> expiredClAndMissingPages : expiredPeersClAndMissingPages.values()) {
      BitSet expiredPeersMissingPages = expiredClAndMissingPages.getSecond();
      expiredPeersMissingPages.set(pageIndex);
    }
    return needSet;
  }

  public void freePage(int pageIndex) {
    if (!metadata.isPageFree(pageIndex)) {
      logger.debug("freePage {}", pageIndex);
      metadata.clearPage(pageIndex);
      metadata.setBitmapNeedPersisted(true);
    }
  }

  public boolean isInnerMigrating() {
    synchronized (innerMigratingLock) {
      return innerMigrating;
    }
  }

  public PageAddress getLogicalPageAddressToApplyLog(int pageIndex) {
    synchronized (innerMigratingLock) {
      if (innerMigrating) {
        if (metadata.getBitmap()
            .get(pageIndex, SegmentUnitBitmap.SegmentUnitBitMapType.Migration)) {
          return PageAddressGenerator
              .generate(getSegId(), metadataOnMigratingDestination.getLogicalDataOffset(),
                  pageIndex,
                  archiveOfMigratingDestination.getStorage(), ArchiveOptions.PAGE_SIZE);
        }
      }
      return PageAddressGenerator
          .generate(getSegId(), metadata.getLogicalDataOffset(), pageIndex, archive.getStorage(),
              ArchiveOptions.PAGE_SIZE);
    }
  }

  RawArchive getRawArchiveByPageAddress(PageAddress pageAddress) {
    RawArchive myArchive;
    synchronized (innerMigratingLock) {
      if (innerMigrating && pageAddress.getStorage()
          .equals(archiveOfMigratingDestination.getStorage())) {
        myArchive = archiveOfMigratingDestination;
      } else {
        myArchive = archive;
      }
    }
    return myArchive;
  }

  @Override
  public Storage getStorage() {
    return archive.getStorage();
  }

  @Override
  public long getSegmentUnitDataStartPosition() {
    return archive.getSegmentUnitDataStartPosition();
  }

  public long getSegmentUnitLogicalDataStartPosition() {
    return archive.getSegmentUnitLogicalDataStartPosition();
  }

  public SegmentUnitMultiAddressManager getAddressManager() {
    return multiAddressManager;
  }

  public boolean isMigrating() {
    return metadata.isMigratedStatus();
  }

  public boolean isPauseVotingProcess() {
    return pauseVotingProcess;
  }

  public void setPauseVotingProcess(boolean pauseVotingProcess) {
    this.pauseVotingProcess = pauseVotingProcess;
  }

  public void setPauseCatchUpLogForSecondary(boolean value) {
    logger
        .warn("set pause catch up log for secondary to new value {}, old value {}, segId {}", value,
            isPausedCatchUpLogForSecondary.get(), getSegId());
    isPausedCatchUpLogForSecondary.set(value);
    if (!value) {
      drivePlalLease.extend();
    }
  }

  public boolean isPausedCatchUpLogForSecondary() {
    return isPausedCatchUpLogForSecondary.get();
  }

  public void setPrimaryClIdAndCheckNeedPclDriverOrNot(long primaryClId) {
    if (null == this.getSegmentLogMetadata()) {
      return;
    }

    logger.debug("begin update primary cl id {}, old id {}, segId {}", primaryClId,
        this.getSegmentLogMetadata().getPrimaryClId(), getSegId());
    if (isPrimaryClSetting.compareAndSet(false, true)) {
      try {
        if (this.getSegmentLogMetadata().getPrimaryClId() < primaryClId) {
          this.getSegmentLogMetadata().setPrimaryClId(primaryClId);
        }

        logger.debug("begin check if need pcl drive call {}",
            isPausedCatchUpLogForSecondary.get());
        if (!isPausedCatchUpLogForSecondary.get()) {
          try {
            if (secondaryPclDriver.call()) {
              logger.info("begin a sync log task {}", getSegId());
            }
          } catch (Exception e) {
            logger.error("pcl driver got some exception {}", getSegId(), e);
          } finally {
            drivePlalWhenLeaseTimeout();
          }
        }
      } finally {
        isPrimaryClSetting.compareAndSet(true, false);
      }
    }

  }

  public boolean beginProcessPclDriverForSecondary() {
    if (isProcessingPclDriverForSecondary.compareAndSet(false, true)) {
      return true;
    } else {
      return false;
    }
  }

  public void finishProcessPclDriverForSecondary() {
    isProcessingPclDriverForSecondary.set(false);
  }

  public void updateDrivePlalLease() {
    drivePlalLease.extend();
  }

  private void drivePlalWhenLeaseTimeout() {
    if (drivePlalLease.expire()) {
      if (this.getSegmentLogMetadata().getClId() > this.getSegmentLogMetadata().getLalId()) {
        try {
          PlalDriver.drive(this.getSegmentLogMetadata(), this, plalEngine);
        } catch (Throwable t) {
          logger.warn("fail to drive plal for segment {} ", this.getSegId(), t);
        } finally {
          updateDrivePlalLease();
        }
      }
    }
  }

  public void setPageHasBeenWrittenAndFirstWritePageIndex(int pageIndex) {
    firstWriteLock.lock();
    try {
      boolean firstMark = setPageHasBeenWritten(pageIndex);
      if (firstMark) {
        addFirstWritePageIndex(pageIndex);
      }
    } finally {
      firstWriteLock.unlock();
    }

  }

  private void addFirstWritePageIndex(int pageIndex) {
    if (metadata.getSegmentUnitType() != SegmentUnitType.Flexible) {
      firstWritePageIndex.add(pageIndex);
    }
  }

  public void removeFirstWritePageIndex(int pageIndex) {
    firstWriteLock.lock();
    try {
      firstWritePageIndex.remove(pageIndex);
    } finally {
      firstWriteLock.unlock();
    }

  }

  public void clearFirstWritePageIndex() {
    firstWriteLock.lock();
    try {
      firstWritePageIndex.clear();
    } finally {
      firstWriteLock.unlock();
    }

  }

  public boolean isPageIndexFristWrite(int pageIndex) {
    return firstWritePageIndex.contains(pageIndex);
  }

  public void clearAvailableSecondary() {
    availableSecondary = ImmutableSet.of();
  }

  public Set<EndPoint> getAvailableSecondary() {
    return availableSecondary;
  }

  public void updateAvailableSecondary(Collection<EndPoint> availableSecondary) {
    this.availableSecondary = ImmutableSet.copyOf(availableSecondary);
  }

  public boolean isForceFullCopy(DataNodeConfiguration configuration,
      SegmentMembership currentMembership, InstanceId myself) {
    boolean forceFullCopy =
        configuration.isForceFullCopy() && !currentMembership.getSecondaries().contains(myself);
    if (forceFullCopy) {
      logger.warn("wonder force full copy, membership {} {}", currentMembership, this.getSegId());
    }

    MigrationStatus migrationStatus = getSegmentUnitMetadata().getMigrationStatus();
    if (migrationStatus.isMigratedStatus()) {
      logger.warn("the wrong migration status={}, must migrate, full copy={}", migrationStatus,
          getSecondaryCopyPageManager() == null ? false
              : getSecondaryCopyPageManager().isFullCopy());

      forceFullCopy = true;
    }

    return forceFullCopy;
  }

  public boolean secondaryClIdExistInPrimary(InstanceId secondaryId, long clIdFromSecondary,
      long plIdFromSecondary, LogStorageReader logStorageReader, boolean isForceFullCopy) {
    boolean secondaryClIdExistInPrimary;
    SegmentMembership membership = getSegmentUnitMetadata().getMembership();
    try {
      if (isForceFullCopy) {
        logger.warn("instance {}, {} force full copy", secondaryId, getSegId());
        secondaryClIdExistInPrimary = false;
      } else if (membership.isSecondaryCandidate(secondaryId)) {
        logger.warn("{} {} secondary candidate {}, full copy is needed", secondaryId, getSegId(),
            membership, clIdFromSecondary);
        secondaryClIdExistInPrimary = false;
      } else if (clIdFromSecondary >= segmentLogMetadata.getPlId() || logStorageReader
          .logIdExists(getSegId(), clIdFromSecondary)) {
        logger.warn("instance {}, {} doesn't need full copy", secondaryId, getSegId());
        secondaryClIdExistInPrimary = true;
      } else {
        logger.warn("instance {}, {} secondary cl not exists in primary, full copy is needed",
            secondaryId, getSegId());
        secondaryClIdExistInPrimary = false;
      }
    } catch (Exception e) {
      logger.error(
          "can't read log storage for segment {} log id after {}."
              + " Let the secondary copy all pages.",
          getSegId(), plIdFromSecondary, e);
      secondaryClIdExistInPrimary = false;
    }

    return secondaryClIdExistInPrimary;
  }

  public long getLastMinRecoverLogId() {
    return lastMinRecoverLogId.get();
  }

  public boolean setLastMinRecoverLogId(long lastMinRecoverLogId) {
    do {
      long last = this.lastMinRecoverLogId.get();
      if (last > lastMinRecoverLogId) {
        if (this.lastMinRecoverLogId.compareAndSet(last, lastMinRecoverLogId)) {
          return true;
        }
      } else {
        return false;
      }
    } while (true);
  }

  public class ShadowInfo {
    public final PageAddress originalPageAddress;
    public final int lastSnapshotId;

    public ShadowInfo(PageAddress originalPageAddress, int lastSnapshotId) {
      this.originalPageAddress = originalPageAddress;
      this.lastSnapshotId = lastSnapshotId;
    }

  }
}
