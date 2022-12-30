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

package py.datanode.segment.heartbeat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.archive.segment.SegId;
import py.archive.segment.SegmentUnitStatus;
import py.client.thrift.GenericThriftClientFactory;
import py.common.Utils;
import py.common.struct.Pair;
import py.datanode.configuration.DataNodeConfiguration;
import py.datanode.segment.SegmentUnit;
import py.datanode.segment.datalog.LogImage;
import py.exception.GenericThriftClientFactoryException;
import py.instance.Instance;
import py.instance.InstanceId;
import py.instance.InstanceStore;
import py.instance.PortType;
import py.thrift.datanode.service.DataNodeService;
import py.thrift.datanode.service.HeartbeatRequest;
import py.thrift.datanode.service.HeartbeatRequestUnit;
import py.thrift.datanode.service.HeartbeatResponse;
import py.thrift.datanode.service.HeartbeatResponseUnit;
import py.thrift.datanode.service.HeartbeatResultThrift;

public class PrimaryHeartGroup implements Delayed, Runnable {
  private static final Logger logger = LoggerFactory.getLogger(PrimaryHeartGroup.class);
  // Store the heart beat list, when it need to heartbeat host, no need to re-construct the list
  private final Map<SegId, Pair<HeartbeatRequestUnit, SegmentUnit>> primaryHeartbeatMap = 
      new ConcurrentHashMap<>();
  private boolean newCreated;
  private long lastTimeRun = 0;
  private InstanceId hostToHeartbeat;
  private InstanceStore instanceStore;
  private DataNodeConfiguration dataNodeCfg;
  private DelayQueue<PrimaryHeartGroup> hostHeartbeatQueue;
  private GenericThriftClientFactory<DataNodeService.Iface> dataNodeSyncClientFactory;

  public PrimaryHeartGroup(InstanceId hostToHeartbeat, InstanceStore instanceStore,
      DataNodeConfiguration dataNodeCfg,
      DelayQueue<PrimaryHeartGroup> hostHeartbeatQueue,
      GenericThriftClientFactory<DataNodeService.Iface> dataNodeSyncClientFactory) {
    this.hostToHeartbeat = hostToHeartbeat;
    this.instanceStore = instanceStore;
    this.dataNodeCfg = dataNodeCfg;
    this.hostHeartbeatQueue = hostHeartbeatQueue;
    this.dataNodeSyncClientFactory = dataNodeSyncClientFactory;
    newCreated = true;
  }

  public boolean addPrimary(SegmentUnit segUnit) {
    if (isBeingRemoved()) {
      return false;
    }

    HeartbeatRequestUnit unit = new HeartbeatRequestUnit();
    unit.setEpoch(segUnit.getSegmentUnitMetadata().getMembership().getSegmentVersion().getEpoch());
    unit.setVolumeId(segUnit.getSegId().getVolumeId().getId());
    unit.setSegIndex(segUnit.getSegId().getIndex());
    if (null != segUnit.getSegmentLogMetadata()) {
      unit.setPcl(segUnit.getSegmentLogMetadata().getClId());
    } else {
      unit.setPcl(LogImage.INVALID_LOG_ID);
    }
    Pair<HeartbeatRequestUnit, SegmentUnit> heartbeatRequestAndSegUnit = new Pair<>();

    heartbeatRequestAndSegUnit.setFirst(unit);
    heartbeatRequestAndSegUnit.setSecond(segUnit);

    synchronized (this) {
      if (isBeingRemoved()) {
        return false;
      }
      primaryHeartbeatMap.put(segUnit.getSegId(), heartbeatRequestAndSegUnit);
      newCreated = false;
      return true;
    }
  }

  public void removePrimary(SegmentUnit segUnit) {
    synchronized (this) {
      primaryHeartbeatMap.remove(segUnit.getSegId());
    }
  }

  private boolean isBeingRemoved() {
    return !newCreated && getGroupCount() == 0;
  }

  public int getGroupCount() {
    return primaryHeartbeatMap.size();
  }

  public long getLastTimeRun() {
    return lastTimeRun;
  }

  public void setLastTimeRun(long lastTimeRun) {
    this.lastTimeRun = lastTimeRun;
  }

  @Override
  public int compareTo(Delayed o) {
    Validate.isTrue(o instanceof PrimaryHeartGroup);

    PrimaryHeartGroup other = (PrimaryHeartGroup) o;
    long diff = this.lastTimeRun - other.lastTimeRun;
    return (diff == 0L) ? 0 : (diff > 0L ? 1 : -1);
  }

  @Override
  public long getDelay(TimeUnit unit) {
    long now = System.currentTimeMillis();
    return (lastTimeRun + dataNodeCfg.getRateOfExtendingLeaseMs() - now);
  }

  public InstanceId getInstance() {
    return hostToHeartbeat;
  }

  public void setInstance(InstanceId instance) {
    this.hostToHeartbeat = instance;
  }

  @Override
  public String toString() {
    return "PrimaryHeartGroup[instance=" + this.hostToHeartbeat + ", lastTimeRun=" + Utils
        .millsecondToString(this.lastTimeRun) + ", rateOfExtendingLeaseMs=" + dataNodeCfg
        .getRateOfExtendingLeaseMs() + ", heartbeatList=" + primaryHeartbeatMap + "]";
  }

  @Override
  public void run() {
    long currentTime = System.currentTimeMillis();
    if (getGroupCount() == 0) {
      logger.warn("No need to heart beat with {} anymore, for not secondary in the host",
          hostToHeartbeat);
      return;
    }

    Instance hostToHearbeat = instanceStore.get(hostToHeartbeat);
    HeartbeatRequest request = null;
    try {
      if (hostToHearbeat != null) {
        List<HeartbeatRequestUnit> requestUnits = new ArrayList<>();
        primaryHeartbeatMap.values().forEach(requestUnitAndSegUnit -> {
          HeartbeatRequestUnit requestUnit = requestUnitAndSegUnit.getFirst();
          SegmentUnit segUnit = requestUnitAndSegUnit.getSecond();
          SegmentUnitStatus segmentUnitStatus = segUnit.getSegmentUnitMetadata().getStatus();
          if (segmentUnitStatus.isPrimary()) {
            requestUnit.setPrePrimary(segUnit.getSegmentUnitMetadata().getStatus()
                == SegmentUnitStatus.PrePrimary);
            requestUnit.setPcl(null == segUnit.getSegmentLogMetadata() 
                ? LogImage.INVALID_LOG_ID
                : requestUnitAndSegUnit.getSecond().getSegmentLogMetadata().getClId());
            requestUnits.add(requestUnit);
          }
        });
        request = new HeartbeatRequest(currentTime, requestUnits);

        DataNodeService.Iface client;
        try {
          client = dataNodeSyncClientFactory
              .generateSyncClient(hostToHearbeat.getEndPointByServiceName(PortType.HEARTBEAT),
                  dataNodeCfg.getExtensionRequestTimeOutMs(),
                  dataNodeCfg.getExtensionConnectionTimeoutMs());
        } finally {
          logger.info("nothing need to do here");
        }

        HeartbeatResponse response;
        try {
          response = client.heartbeat(request);
          logger.debug("heartbeat response {}", response);
        } finally {
          dataNodeSyncClientFactory.releaseSyncClient(client);
        }

        Map<SegId, HeartbeatResponseUnit> failedUnits = new HashMap<SegId, HeartbeatResponseUnit>();
        synchronized (this) {
          for (HeartbeatResponseUnit unit : response.getHeartbeatResponseUnits()) {
            SegId segId = new SegId(unit.getVolumeId(), unit.getSegIndex());
            Pair<HeartbeatRequestUnit, SegmentUnit> requestUnitAndSegUnit = primaryHeartbeatMap
                .get(segId);
            if (requestUnitAndSegUnit == null) {
              logger.debug(
                  "Wow, you are in good luck that the segment has been removed from the hashmap {}",
                  segId);
              continue;
            }

            requestUnitAndSegUnit.getSecond()
                .saveHeartbeatResult(hostToHeartbeat, unit.getResult());
            failedUnits.put(segId, unit);
          }

          primaryHeartbeatMap.values().forEach(requestUnitAndSegUnit -> {
            SegmentUnit segUnit = requestUnitAndSegUnit.getSecond();
            HeartbeatResponseUnit failUnit = failedUnits.get(segUnit.getSegId());
            if (failUnit == null) {
              segUnit.extendPeerLease(dataNodeCfg.getSecondaryLeaseInPrimaryMs(), hostToHeartbeat);
              segUnit.saveHeartbeatResult(hostToHeartbeat, HeartbeatResultThrift.Success);
            } else {
              logger
                  .warn("seg id={} response with an exception={}, not extend lease, instanceId={}",
                      segUnit.getSegId(), failUnit, hostToHeartbeat);
            }
          });
        }
      }
    } catch (GenericThriftClientFactoryException e) {
      logger.info("can not get client with host: {}, carry info:{}", hostToHearbeat, request,
          e);
    } catch (Exception e) {
      logger.warn("can not extend the lease with host={}, carry info:{}, ", hostToHearbeat, request,
          e);
    } finally {
      this.lastTimeRun = System.currentTimeMillis();
      hostHeartbeatQueue.put(this);
    }
  }

  public List<HeartbeatRequestUnit> getHeartbeatRequestUnitList() {
    List<HeartbeatRequestUnit> requestUnits = new ArrayList<>();
    primaryHeartbeatMap.values().forEach(
        requestUnitAndSegUnit -> {
          requestUnitAndSegUnit.getFirst().setPcl(
              null == requestUnitAndSegUnit.getSecond().getSegmentLogMetadata() 
                  ? LogImage.INVALID_LOG_ID
                  : requestUnitAndSegUnit.getSecond().getSegmentLogMetadata().getClId());
          requestUnits.add(requestUnitAndSegUnit.getFirst());
        });
    return requestUnits;
  }
}
